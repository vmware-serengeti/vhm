package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;

public class VMHostChangeEvent extends NotificationEvent implements ClusterStateChangeEvent {
   String _vmMoRef;
   String _hostMoRef;
   
   public VMHostChangeEvent(String vmMoRef, String hostMoRef) {
      super(false, false);
      _vmMoRef = vmMoRef;
      _hostMoRef = hostMoRef;
   }

   public String getVmMoRef() {
      return _vmMoRef;
   }

   public String getHostMoRef() {
      return _hostMoRef;
   }

}
