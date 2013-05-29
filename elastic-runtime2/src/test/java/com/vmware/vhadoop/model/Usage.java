package com.vmware.vhadoop.model;


public interface Usage
{

   /**
    * Returns the allocated memory in megabytes
    * @return consumed memory in Mb
    */
   public long getMemoryUsage();

   /**
    * Returns the memory that's actively being used at a given point
    * @return active memory in Mb
    */
   public long getActiveMemory();

   /**
    * Returns the CPU allocation in Mhz
    * @return cpu usage in Mhz
    */
   public long getCpuUsage();
}