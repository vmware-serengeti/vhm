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

import java.util.List;

import com.vmware.vhadoop.model.Host;
import com.vmware.vhadoop.model.Orchestrator;
import com.vmware.vhadoop.model.VM;

public class BasicScenario
{

   /**
    * Constructs a symmetrical scenario with a given number of hosts and VMs per host
    * @param hosts - number of hosts
    * @param vms - vms per host
    */
   public static Orchestrator getOrchestrator(int hosts, int vms) {
      Orchestrator orchestrator = new Orchestrator("BasicScenario");

      for (int i = 0; i < hosts; i++) {
         /* 8x2Ghz cpus, 24Gb memory */
         Host host = new Host("host"+i, 16000, 24*1024, null);
         orchestrator.add(host);

         /* 2x2Ghz cpus, 6Gb Memory */
         for (int j = 0; j < vms; j++) {
            VM vm = new ConstantWorkloadVM("vm"+((i*vms)+j), 4000, 6*1024, null);
            vm.powerOn();
            host.add(vm);
         }
      }

      return orchestrator;
   }

   public static void main(String args[]) {
      Orchestrator orchestrator = BasicScenario.getOrchestrator(4, 4);
      @SuppressWarnings("unchecked")
      List<VM> vms = (List<VM>)orchestrator.get(VM.class);
      for (VM vm : vms) {
         vm.powerOn();
      }

      /* report usage */
      System.out.println(orchestrator.report());
   }
}
