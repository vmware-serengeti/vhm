package com.vmware.vhadoop.model;

public interface Limits
{
   public static final long UNLIMITED = -1;
   /**
    * Returns the allocated memory in megabytes
    * @return
    */
   public long getMemoryLimit();

   /**
    * Returns the CPU allocation in Mhz
    * @return
    */
   public long getCpuLimit();

   /**
    * Sets the allocated memory in megabytes
    * @param allocation
    */
   void setMemoryLimit(long allocation);

   /**
    * Sets the CPU allocation in Mhz
    * @param allocation
    */
   void setCpuLimit(long allocation);
}