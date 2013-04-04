package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.vhm.events.VMUpdatedEvent;
import com.vmware.vhadoop.vhm.events.VMRemovedFromClusterEvent;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterStateChangeListenerImpl extends AbstractClusterMapReader implements com.vmware.vhadoop.api.vhm.ClusterStateChangeListener {
   private static final Logger _log = Logger.getLogger("ChangeListener");
   private static final int backoffPeriodMS = 60000; // 1 minute in milliseconds

   EventConsumer _eventConsumer;
   VCActions _vcActions;
   String _serengetiFolderName;
   
   public ClusterStateChangeListenerImpl(VCActions vcActions, String serengetiFolderName) {
      _vcActions = vcActions;
      _serengetiFolderName = serengetiFolderName;
   }
   
   public static class TestCluster {
      public TestCluster() {
         this._vms = new HashSet<VMEventData>();
      }
      public Set<VMEventData> _vms;

      public TestCluster addComputeVM(String vmMoRef, String hostMoRef, String serengetiUUID, String masterUUID, String masterMoRef, boolean powerState) {
         VMEventData cved = new VMEventData();
         cved._vmMoRef = vmMoRef;
         cved._isLeaving = false;
         
         cved._myName = vmMoRef;
         cved._myUUID = vmMoRef;
         cved._masterUUID = masterUUID;
         cved._masterMoRef = masterMoRef;
         cved._isElastic = true;

         cved._powerState = powerState;
         cved._hostMoRef = hostMoRef;
         cved._serengetiFolder = serengetiUUID;

         _vms.add(cved);
         return this;
      }

      public TestCluster addMasterVM(String vmMoRef, String hostMoRef, String serengetiUUID, String UUID, boolean enableAutomation, int minInstances, boolean powerState) {
         VMEventData mved = new VMEventData();
         mved._vmMoRef = vmMoRef;
         mved._isLeaving = false;
         
         mved._myName = vmMoRef;
         mved._myUUID = UUID;
         mved._masterUUID = UUID;
         mved._isElastic = false;

         mved._powerState = powerState;
         mved._hostMoRef = hostMoRef;
         mved._serengetiFolder = serengetiUUID;

         mved._enableAutomation = enableAutomation;
         mved._minInstances = minInstances;
         _vms.add(mved);
         return this;
      }
   }
   
   
   @Override
   public void registerEventConsumer(EventConsumer consumer) {
      _eventConsumer = consumer;
   }

   void discoverTestCluster(TestCluster cluster) {
      for (VMEventData vmEvent : cluster._vms) {
         _eventConsumer.placeEventOnQueue(new VMUpdatedEvent(vmEvent));
      }
   }
   
   @Override
   public void start() {
      new Thread(new Runnable() {
         @Override
         public void run() {
            String version = "";
            while (true) {
               ArrayList<VMEventData> vmDataList = new ArrayList<VMEventData>(); 
               version = _vcActions.waitForPropertyChange(_serengetiFolderName, version, vmDataList);
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
                     _log.log(Level.INFO, "Detected change moRef= " + vmData._vmMoRef + " leaving= " + vmData._isLeaving);

                     if (vmData._isLeaving) {
                        _eventConsumer.placeEventOnQueue(new VMRemovedFromClusterEvent(vmData._vmMoRef));
                     } else {
                        _eventConsumer.placeEventOnQueue(new VMUpdatedEvent(vmData));
                     }
                  }
               }
            }
         }}, "ClusterSCL_Poll_Thread").start();
   }
}
