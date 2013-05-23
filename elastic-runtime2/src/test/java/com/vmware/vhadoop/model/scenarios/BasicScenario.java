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
