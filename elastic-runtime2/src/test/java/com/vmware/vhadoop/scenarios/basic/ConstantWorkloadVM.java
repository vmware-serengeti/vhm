package com.vmware.vhadoop.scenarios.basic;

import com.vmware.vhadoop.model.Orchestrator;
import com.vmware.vhadoop.model.VM;
import com.vmware.vhadoop.scenarios.ConstantWorkload;

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
