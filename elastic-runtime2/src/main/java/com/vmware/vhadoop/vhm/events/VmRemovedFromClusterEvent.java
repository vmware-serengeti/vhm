package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;

public class VmRemovedFromClusterEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   private String _vmId;
   
   public VmRemovedFromClusterEvent(String vmId) {
      super(false, false);
      _vmId = vmId;
   }

   public String getVmId() {
      return _vmId;
   }
}
