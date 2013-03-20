package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.vhm.events.VMUpdatedEvent;
import com.vmware.vhadoop.vhm.events.VMRemovedFromClusterEvent;
import com.vmware.vhadoop.vhm.vc.VCTestModel;

import java.util.*;

public class ClusterStateChangeListenerImpl extends AbstractClusterMapReader implements com.vmware.vhadoop.api.vhm.ClusterStateChangeListener {

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
               for (VMEventData vmData : vmDataList) {
                  System.out.println(Thread.currentThread().getName()+": ClusterStateChangeListener: detected change moRef= "
                        +vmData._vmMoRef + " leaving=" + vmData._isLeaving);
                  
                  if (vmData._isLeaving) {
                     _eventConsumer.placeEventOnQueue(new VMRemovedFromClusterEvent(vmData._vmMoRef));
                  } else {
                     _eventConsumer.placeEventOnQueue(new VMUpdatedEvent(vmData));
                  }
               }
            }
         }}, "ClusterSCL_Poll_Thread").start();
   }
}
