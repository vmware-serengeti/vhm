package com.vmware.vhadoop.model;

abstract public class Workload extends ResourceUsage
{
   /**
    * Stops the workload
    * @param b - force stop if true, shutdown cleanly if false
    */
   public void stop(boolean b) {
      setCpuUsage(0);
      setMemoryUsage(0);
   }

   /**
    * Starts the workload running. This commences resource utilization.
    */
   public abstract void start();
}
