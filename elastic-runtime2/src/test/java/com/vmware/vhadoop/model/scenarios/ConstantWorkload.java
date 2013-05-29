package com.vmware.vhadoop.model.scenarios;

import static com.vmware.vhadoop.model.Resource.CPU;
import static com.vmware.vhadoop.model.Resource.MEMORY;

import com.vmware.vhadoop.model.Workload;
import com.vmware.vhadoop.model.allocation.Allocation;
import com.vmware.vhadoop.model.allocation.CpuAllocator;
import com.vmware.vhadoop.model.allocation.MemoryAllocator;

public class ConstantWorkload extends Workload
{
   /** Default proportion of active/committed memory */
   private final static float DEFAULT_ACTIVE_PROPORTION = 0.7f;

   long cpu;
   long memory;

   /**
    * Creates a constant workload with the specified cpu and memory characteristics
    * @param cpu
    * @param memory
    */
   public ConstantWorkload(long cpu, long memory) {
      super("Constant workload");
      this.cpu = cpu;
      this.memory = memory;
   }

   @Override
   public Allocation load(Allocation available) {
      allocation = new Allocation("ConstantWorkload");
      allocation.setResourceUsage(MEMORY, MemoryAllocator.TOTAL, memory);
      allocation.setResourceUsage(CPU, CpuAllocator.TOTAL, cpu);

      return allocation;
   }
}
