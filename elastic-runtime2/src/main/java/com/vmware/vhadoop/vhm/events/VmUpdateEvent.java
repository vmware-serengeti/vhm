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

import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.util.LogFormatter;

public class VmUpdateEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   private final String _vmId;
   private final VMVariableData _variableData;

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

   String getParamListString(Logger logger) {
      String basic = "vmId=<%V"+_vmId+"%V>";
      String detail = LogFormatter.isDetailLogging(logger) ? ", vmVariableData="+_variableData : "";
      return basic+detail;
   }

   @Override
   public String toString(Logger logger) {
      return "VmUpdateEvent{"+getParamListString(logger)+"}";
   }
   
   @Override
   public String toString() {
      return toString(null);
   }
}
