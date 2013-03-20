package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;

public class VMRemovedFromClusterEvent extends NotificationEvent implements ClusterStateChangeEvent {
   String _vmMoRef;
   
   public VMRemovedFromClusterEvent(String vmMoRef) {
      super(false, false);
      _vmMoRef = vmMoRef;
   }

   public String getVmMoRef() {
      return _vmMoRef;
   }

}
