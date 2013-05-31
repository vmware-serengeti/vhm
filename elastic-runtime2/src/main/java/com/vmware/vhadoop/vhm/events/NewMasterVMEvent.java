package com.vmware.vhadoop.vhm.events;

public class NewMasterVMEvent extends NewVmEvent {
   private SerengetiClusterConstantData _clusterConstantData;
   private SerengetiClusterVariableData _clusterVariableData;

   public NewMasterVMEvent(String vmId, String clusterId, VMConstantData constantData, VMVariableData vmVariableData, 
         SerengetiClusterConstantData clusterConstantData, SerengetiClusterVariableData clusterVariableData) {
      super(vmId, clusterId, constantData, vmVariableData);
      _clusterConstantData = clusterConstantData;
      _clusterVariableData = clusterVariableData;
   }
   
   public SerengetiClusterConstantData getClusterConstantData() {
      return _clusterConstantData;
   }

   public SerengetiClusterVariableData getClusterVariableData() {
      return _clusterVariableData;
   }
}
