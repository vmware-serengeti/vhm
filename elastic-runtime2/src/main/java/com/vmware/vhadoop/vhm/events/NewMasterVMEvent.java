/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

package com.vmware.vhadoop.vhm.events;

public class NewMasterVMEvent extends NewVmEvent {
   private final SerengetiClusterConstantData _clusterConstantData;
   private final SerengetiClusterVariableData _clusterVariableData;

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
