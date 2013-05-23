package com.vmware.vhadoop.model.scenarios;

import com.vmware.vhadoop.model.Workload;

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

   /**
    * Starts the workload, starts using resources
    */
   @Override
   public void start() {
      setCpuUsage(cpu);
      setMemoryUsage(memory);
   }

   /**
    * Provides basic assumption that a given proportion of the memory usage is active at any given point
    * @return the active memory in Mb
    */
   @Override
   public long getActiveMemory() {
      return (long) (getMemoryUsage() * DEFAULT_ACTIVE_PROPORTION);
   }
}
