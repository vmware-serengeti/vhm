package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;

public class NewVmEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   private String _vmId;
   private String _clusterId;
   private VMVariableData _variableData;
   private VMConstantData _constantData;
   
   public NewVmEvent(String vmId, String clusterId, VMConstantData constantData, VMVariableData variableData) {
      super(false, false);
      _vmId = vmId;
      _clusterId = clusterId;
      _constantData = constantData;
      _variableData = variableData;
   }
   
   public String getVmId() {
      return _vmId;
   }

   public String getClusterId() {
      return _clusterId;
   }

   public VMVariableData getVariableData() {
      return _variableData;
   }

   public VMConstantData getConstantData() {
      return _constantData;
   }
}
