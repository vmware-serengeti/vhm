package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;

public class VMUpdatedEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   VMEventData _vm;
   
   public VMUpdatedEvent(VMEventData vm) {
      super(false, true);
      _vm = vm;
   }

   public VMEventData getVm() {
      return _vm;
   }

}
