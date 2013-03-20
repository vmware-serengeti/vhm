package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;

public class VMUpdatedEvent extends NotificationEvent implements ClusterStateChangeEvent {
   VMEventData _vm;
   
   public VMUpdatedEvent(VMEventData vm) {
      super(false, false);
      _vm = vm;
   }

   public VMEventData getVm() {
      return _vm;
   }

}
