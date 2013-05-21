package com.vmware.vhadoop.model;

import static com.vmware.vhadoop.model.Resource.CPU;
import static com.vmware.vhadoop.model.Resource.MEMORY;

import java.util.Map;
import java.util.TreeMap;

abstract public class ResourceLimits implements Limits
{
   private Map<Resource,Object> limits;

   /**
    * Creates unlimited resource limits
    */
   ResourceLimits() {
      limits = new TreeMap<Resource,Object>();
      limits.put(MEMORY, -1);
      limits.put(CPU, -1);
   }

   /**
    * Returns the allocated memory in megabytes
    * @return
    */
   @Override
   public long getMemoryLimit() {
      return (Long)(limits.get(MEMORY));
   }

   /**
    * Returns the CPU allocation in Mhz
    * @return
    */
   @Override
   public long getCpuLimit() {
      return (Long)(limits.get(CPU));
   }

   /**
    * Sets the allocated memory in megabytes
    * @param allocation
    */
   @Override
   public void setMemoryLimit(long allocation) {
      limits.put(MEMORY, allocation);
   }

   /**
    * Sets the CPU allocation in Mhz
    * @param allocation
    */
   @Override
   public void setCpuLimit(long allocation) {
      limits.put(CPU, allocation);
   }
}