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
