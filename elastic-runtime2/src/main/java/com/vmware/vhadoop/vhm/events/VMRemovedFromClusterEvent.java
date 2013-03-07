package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;

public class VMRemovedFromClusterEvent extends NotificationEvent implements ClusterStateChangeEvent {
   String _vmMoRef;
   String _clusterId;
   
   public VMRemovedFromClusterEvent(String vmMoRef, String clusterId) {
      super(false, false);
      _vmMoRef = vmMoRef;
      _clusterId = clusterId;
   }

   public String getVmMoRef() {
      return _vmMoRef;
   }

   public String getClusterId() {
      return _clusterId;
   }
}
