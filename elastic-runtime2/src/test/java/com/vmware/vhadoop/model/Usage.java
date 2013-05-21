package com.vmware.vhadoop.model;


public interface Usage
{
   /**
    * Gets the maximum stable interval for the container and it's children. Should be overridden by
    * inheritors who don't have stable resource usage patterns.
    * @return Long.MAX_VALUE
    */
   public long getStableInterval();

   /**
    * Returns the allocated memory in megabytes
    * @return
    */
   public long getMemoryUsage();

   /**
    * Returns the CPU allocation in Mhz
    * @return
    */
   public long getCpuUsage();

   /**
    * Adds a parent container to be notified when our usage changes
    */
   public void addParent(ResourceContainer parent);

   /**
    * Removes a parent container from the list
    */
   public void removeParent(ResourceContainer parent);
}