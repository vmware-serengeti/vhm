package com.vmware.vhadoop.vhm.events;

public class MasterVmUpdateEvent extends VmUpdateEvent {
   private SerengetiClusterVariableData _clusterVariableData;
   
   public MasterVmUpdateEvent(String vmId, VMVariableData variableData, SerengetiClusterVariableData clusterVariableData) {
      super(vmId, variableData);
      _clusterVariableData = clusterVariableData;
   }
   
   public SerengetiClusterVariableData getClusterVariableData() {
      return _clusterVariableData;
   }
}
