package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.vhm.events.VMUpdatedEvent;
import com.vmware.vhadoop.vhm.events.VMRemovedFromClusterEvent;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterStateChangeListenerImpl extends AbstractClusterMapReader implements EventProducer {
   private static final Logger _log = Logger.getLogger("ChangeListener");
   private static final int backoffPeriodMS = 60000; // 1 minute in milliseconds

   EventConsumer _eventConsumer;
   VCActions _vcActions;
   String _serengetiFolderName;
   boolean _started;
   
   public ClusterStateChangeListenerImpl(VCActions vcActions, String serengetiFolderName) {
      _vcActions = vcActions;
      _serengetiFolderName = serengetiFolderName;
   }
   
   @Override
   public void registerEventConsumer(EventConsumer consumer) {
      _eventConsumer = consumer;
   }
   
   @Override
   public void start() {
      _started = true;
      new Thread(new Runnable() {
         @Override
         public void run() {
            String version = "";
            while (_started) {
               ArrayList<VMEventData> vmDataList = new ArrayList<VMEventData>();
               try {
                  version = _vcActions.waitForPropertyChange(_serengetiFolderName, version, vmDataList);
               } catch (InterruptedException e) {
                  /* Almost certainly means that stop has been called */
                  continue;
               }
               if (vmDataList.isEmpty() && (version.equals(""))) {
                  /*
                   *  No data received from VC so far -- can happen if user hasn't created any VMs yet
                   *  Adding a sleep to reduce spam.
                   */
                  try {
                     Thread.sleep(backoffPeriodMS);
                  } catch (InterruptedException e) {
                     // TODO Auto-generated catch block
                     e.printStackTrace();
                  }

               } else {
                  for (VMEventData vmData : vmDataList) {
                     _log.log(Level.INFO, "Detected change in vm <%V" + vmData._vmMoRef + "%V> leaving= " + vmData._isLeaving);

                     if (vmData._isLeaving) {
                        _eventConsumer.placeEventOnQueue(new VMRemovedFromClusterEvent(vmData._vmMoRef));
                     } else {
                        _eventConsumer.placeEventOnQueue(new VMUpdatedEvent(vmData));
                     }
                  }
               }
            }
            _log.info("ClusterStateChangeListener stopping...");
         }}, "ClusterSCL_Poll_Thread").start();
   }
   
   @Override
   public void stop() {
      _started = false;
      _vcActions.interruptWait();
   }
}
