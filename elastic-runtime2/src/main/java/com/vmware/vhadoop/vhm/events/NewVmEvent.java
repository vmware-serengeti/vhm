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

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;

public class NewVmEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   private final String _vmId;
   private final String _clusterId;
   private final VMVariableData _variableData;
   private final VMConstantData _constantData;
   
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
