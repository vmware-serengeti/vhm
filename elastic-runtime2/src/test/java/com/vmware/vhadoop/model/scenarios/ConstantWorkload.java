package com.vmware.vhadoop.model.scenarios;

import com.vmware.vhadoop.vhm.model.Allocation;
import com.vmware.vhadoop.vhm.model.api.ResourceType;
import com.vmware.vhadoop.vhm.model.os.Process;

public class ConstantWorkload extends Process
{
   long cpu;
   long mem;

   protected ConstantWorkload(String name, long cpu, long mem) {
      super(name);
      this.cpu = cpu;
      this.mem = mem;
   }

   @Override
   protected Allocation getDesiredAllocation() {
      Allocation desired = Allocation.zeroed();

      desired.set(ResourceType.MEMORY, mem);
      desired.set(ResourceType.CPU, cpu);
      desired.setDuration(Long.MAX_VALUE);

      return desired;
   }
}
