package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;

public class VMAddedToClusterEvent extends NotificationEvent implements ClusterStateChangeEvent {
   VMEventData _vm;
   
   public VMAddedToClusterEvent(VMEventData vm) {
      super(false, false);
      _vm = vm;
   }

   public VMEventData getVm() {
      return _vm;
   }

}
