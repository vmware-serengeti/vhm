package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;

public class VMRemovedFromClusterEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   String _vmMoRef;
   
   public VMRemovedFromClusterEvent(String vmMoRef) {
      super(false, true);
      _vmMoRef = vmMoRef;
   }

   public String getVmMoRef() {
      return _vmMoRef;
   }

}
