package com.vmware.vhadoop.model.allocation;

import static com.vmware.vhadoop.model.Resource.MEMORY;

public class MemoryAllocator extends Allocator
{
   public final static String TOTAL = "total";

   MemoryAllocator(String id, long ram) {
      super(new Allocation(id+"-ManagedMemoryResources"));
      setManagedResource(MEMORY, TOTAL, false, ram);
   }

   /**
    * Takes a request and returns the actual allocation. This
    */
   @Override
   public Allocation allocate(String id, Allocation requested) {
      long totalRequested = requested.getResourceUsage(MEMORY, TOTAL);
      long totalAvailable = managed.getResourceUsage(MEMORY, TOTAL);

      if (totalRequested <= totalAvailable) {
         managed.setResourceUsage(MEMORY, TOTAL, totalAvailable - totalRequested);
         /* if there's an existing allocation, modify that */
         Allocation allocation = allocations.get(id);
         if (allocation == null) {
            allocation = new Allocation(id);
         }
         allocation.setResourceUsage(MEMORY, TOTAL, totalRequested);

         /* ensure it's recorded if it was a new allocation. This returns a cloned copy that it's safe to modify */
         return super.allocate(id, allocation);
      }

      return null;
   }
}
