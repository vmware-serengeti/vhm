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

package com.vmware.vhadoop.model.scenarios;

import com.vmware.vhadoop.model.Orchestrator;
import com.vmware.vhadoop.model.VM;

public class ConstantWorkloadVM extends VM
{

   ConstantWorkloadVM(String id, long vRAM, long cpu, Orchestrator orchestrator) {
      super(id, vRAM, cpu, orchestrator);
   }

   /**
    * Runs a constant workload size for the full VM on power on
    */
   @Override
   public boolean powerOn() {
      if (super.powerOn()) {
         execute(new ConstantWorkload(getCpuLimit(), getMemoryLimit()));
         return true;
      }

      return false;
   }
}
