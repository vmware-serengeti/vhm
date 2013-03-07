package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;

public class VMPowerStateChangeEvent extends NotificationEvent implements ClusterStateChangeEvent {
   String _vmMoRef;
   boolean _powerState;
   
   public VMPowerStateChangeEvent(String vmMoRef, boolean powerState) {
      super(false, false);
      _vmMoRef = vmMoRef;
      _powerState = powerState;
   }

   public String getVmMoRef() {
      return _vmMoRef;
   }

   public boolean getNewPowerState() {
      return _powerState;
   }

}
