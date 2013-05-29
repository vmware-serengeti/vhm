package com.vmware.vhadoop.model.allocation;

public class CpuAllocator extends Allocator
{


   public static final String TOTAL = "total";

   CpuAllocator(Allocation managedResources) {
      super(managedResources);
   }

   @Override
   public Allocation getAvailableResources() {
      return null;
   }

}
