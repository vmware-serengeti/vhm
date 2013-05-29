package com.vmware.vhadoop.model.allocation;

import static com.vmware.vhadoop.model.Resource.MEMORY;

public class VirtualMemoryAllocator extends MemoryAllocator
{
   public final static String SWAPPED = "swapped";
   public final static String PINNED = "pinned";
   public final static String COMMITTED = "committed";

   public VirtualMemoryAllocator(String id, long ram) {
      super(id, ram);

      setManagedResource(MEMORY, SWAPPED, false, 0L);
      setManagedResource(MEMORY, PINNED, false, 0L);
      setManagedResource(MEMORY, COMMITTED, false, 0L);
   }

   /**
    * The available resources. This adjusts for any pinned memory.
    */
   @Override
   public Allocation getAvailableResources() {
      Allocation available = managed.clone();
      long pinned = managed.getResourceUsage(MEMORY, PINNED);
      long total = managed.getResourceUsage(MEMORY, TOTAL);

      available.setResourceUsage(MEMORY, PINNED, total - pinned);

      return available;
   }
}
