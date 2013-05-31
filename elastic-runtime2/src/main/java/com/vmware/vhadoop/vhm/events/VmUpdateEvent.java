package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;

public class VmUpdateEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   private String _vmId;
   private VMVariableData _variableData;

   public VmUpdateEvent(String vmId, VMVariableData variableData) {
      super(false, false);
      _vmId = vmId;
      _variableData = variableData;
   }

   public VMVariableData getVariableData() {
      return _variableData;
   }

   public String getVmId() {
      return _vmId;
   }
}
