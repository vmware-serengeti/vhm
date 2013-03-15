package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.ComputeVMEventData;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.MasterVMEventData;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.PropertyChangeEvent;
import com.vmware.vhadoop.vhm.events.VMAddedToClusterEvent;
import com.vmware.vhadoop.vhm.events.VMPowerStateChangeEvent;

import java.util.*;

public class ClusterStateChangeListenerImpl extends AbstractClusterMapReader implements com.vmware.vhadoop.api.vhm.ClusterStateChangeListener {

   EventConsumer _eventConsumer;
   VCActions _vcActions;
   
   public ClusterStateChangeListenerImpl(VCActions vcActions) {
      _vcActions = vcActions;
   }
   
   public static class TestCluster {
      public TestCluster() {
         this._vms = new HashSet<VMEventData>();
      }
      public Set<VMEventData> _vms;
      
      public TestCluster addComputeVM(String vmMoRef, String hostMoRef, String serengetiUUID, String masterUUID, String masterMoRef, boolean powerState) {
         ComputeVMEventData cved = new ComputeVMEventData();
         cved._vmMoRef = vmMoRef;
         cved._hostMoRef = hostMoRef;
         cved._serengetiUUID = serengetiUUID;
         cved._masterUUID = masterUUID;
         cved._masterMoRef = masterMoRef;
         cved._powerState = powerState;
         _vms.add(cved);
         return this;
      }

      public TestCluster addMasterVM(String vmMoRef, String hostMoRef, String serengetiUUID, String UUID, boolean enableAutomation, int minInstances, boolean powerState) {
         MasterVMEventData mved = new MasterVMEventData();
         mved._vmMoRef = vmMoRef;
         mved._hostMoRef = hostMoRef;
         mved._serengetiUUID = serengetiUUID;
         mved._enableAutomation = enableAutomation;
         mved._minInstances = minInstances;
         mved._powerState = powerState;
         mved._masterUUID = UUID;
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
         _eventConsumer.placeEventOnQueue(new VMAddedToClusterEvent(vmEvent));
      }
   }
   
   private ClusterStateChangeEvent processPropertyChangeEvent(PropertyChangeEvent pce) {
      if (pce.getPropertyKey().equals(VCTestModel.PROPERTY_KEY_POWER_STATE)) {
         return new VMPowerStateChangeEvent(pce.getMoRef(), pce.getNewValue().equalsIgnoreCase("true"));
      }
      return null;
   }
   
   @Override
   public void start() {
      new Thread(new Runnable() {
         @Override
         public void run() {
            while (true) {
               PropertyChangeEvent pce = _vcActions.waitForPropertyChange();
               System.out.println(Thread.currentThread().getName()+": ClusterStateChangeListener: detected change "+
                     pce.getPropertyKey()+" in moRef "+pce.getMoRef()+" to value "+pce.getNewValue());
               _eventConsumer.placeEventOnQueue(processPropertyChangeEvent(pce));
            }
         }}, "ClusterSCL_Poll_Thread").start();
   }
}
