package com.vmware.vhadoop.model;

import static com.vmware.vhadoop.model.Resource.CPU;
import static com.vmware.vhadoop.model.Resource.MEMORY;

import com.vmware.vhadoop.model.allocation.Allocation;
import com.vmware.vhadoop.model.allocation.CpuAllocator;
import com.vmware.vhadoop.model.allocation.MemoryAllocator;

abstract public class ResourceLimits implements Limits
{
   protected Allocation limits;

   /**
    * Creates unlimited resource limits
    */
   ResourceLimits() {
      limits = new Allocation("Limits");
   }

   /**
    * Returns the allocated memory in megabytes
    * @return
    */
   @Override
   public long getMemoryLimit() {
      return limits.getResourceUsage(MEMORY, MemoryAllocator.TOTAL);
   }

   /**
    * Returns the CPU allocation in Mhz
    * @return
    */
   @Override
   public long getCpuLimit() {
      return limits.getResourceUsage(CPU, CpuAllocator.TOTAL);
   }

   /**
    * Sets the allocated memory in megabytes
    * @param allocation
    */
   @Override
   public void setMemoryLimit(long allocation) {
      limits.setResourceUsage(MEMORY, MemoryAllocator.TOTAL, allocation);
   }

   /**
    * Sets the CPU allocation in Mhz
    * @param allocation
    */
   @Override
   public void setCpuLimit(long allocation) {
      limits.setResourceUsage(CPU, CpuAllocator.TOTAL, allocation);
   }
}