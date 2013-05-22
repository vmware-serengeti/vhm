package com.vmware.vhadoop.model;


public class VM extends ResourceContainer
{
   boolean power = false;

   long balloon = 0;
   long balloonTarget = 0;
   long swapped = 0;
   long hostSwapped = 0;

   long committedMemory = 0;

   /**
    * This creates a new VM with the specifed configuration and management orchestration
    * @param orchestrator - the orchestrator that causes changes to propogate through the model
    * @param vRAM - the max memory statically configured for the VM
    * @param cpu - the max memory configured for the VM
    */
   protected VM(String id, long cpu, long vRAM, Orchestrator orchestrator) {
      super(id, orchestrator);

      setCpuLimit(cpu);
      setMemoryLimit(vRAM);
   }

   public void execute(Workload workload) {
      add(workload);
      workload.start();
   }

   /**
    * Reconfigure the VM with new vRam and CPU figures, then power on
    * @param config
    */
   public boolean powerOn(Limits config) {
      /* TODO: if we ever model this as taking time, we need to update the Futures in Orchestrator */
      power = true;

      setCpuLimit(config.getCpuLimit());
      setMemoryLimit(config.getMemoryLimit());

      /* TODO: create a 'bootup' workload that runs immediately after poweron */

      return true;
   }

   /**
    * Power on the VM using its existing configuration
    */
   public boolean powerOn() {
      return this.powerOn(this);
   }

   /**
    * Power off the VM. This also terminates any workloads that are running
    */
   public boolean powerOff() {
      for (ResourceUsage usage : usages) {
         if (usage instanceof Workload) {
            Workload workload = (Workload)usage;
            workload.stop(true);
         }
      }


      usages.clear();
      orchestrator.update();

      /* TODO: if we ever model this as taking time, we need to update the Futures in Orchestrator */
      power = false;

      return true;
   }

   /**
    * Memory usage equates to memory held by the OS. This only goes down by virtue of ballooning.
    * The actual usage of a workload only serves to deflate the balloon and contribute to active.
    * @return the current memory usage (committed - balloon)
    */
   @Override
   public long getMemoryUsage() {
      long usage = super.getMemoryUsage();
      long limit = getMemoryLimit();
      if (usage > limit) {
         long swapTarget = usage - limit;
         if (swapTarget < swapped) {
            swapin(swapped - swapTarget);
         } else {
            swapout(swapTarget - swapped);
         }

         usage = limit;
      } else {
         swapin(swapped);
      }

      if (usage > committedMemory) {
         committedMemory = usage;
      }

      return committedMemory - balloon;
   }

   public long getBalloonSize() {
      return balloon;
   }

   /**
    * Sets the balloon target size for the VM
    * @param target - target size in Mb
    * @return the size of the resulting balloon
    */
   public long setBalloonTarget(long target) {
      long active = getActiveMemory();
      long limit = getMemoryLimit();

      if (target < balloon) {
         /* we're can deflate the balloon */
         long pressure = active - (limit - balloon);
         if (pressure > 0) {
            /* we have the memory pressure to deflate the balloon */
            balloon-= Math.min(pressure, balloon - target);
         } else if (swapped > 0) {
            /* we've now got some free memory, so assume that we can swap in */
            long delta = Math.min(swapped, balloon - target);
            swapin(delta);
            balloon-= delta;
         }

      } else if (target > balloon) {
         /* see how much we can balloon - we work on the assumption that the entire VM vRAM is committed at this point */
         long available = limit - active;
         balloon = available;

         /* TODO: this is where we'd be swapping out if the balloon pressure caused us to evict running processes. See whether we're favouring host or guest swapping */
      }

      return balloon;
   }

   private void swapin(long mb) {
      swapped-= mb;
   }

   private void swapout(long mb) {
      swapped+= mb;
   }

   /**
    * VM basic toString description that includes power state, balloon and swap sizes
    */
   @Override
   public String toString() {
      return super.toString() + ", power: "+(power ? "on" : "off")+", balloon: "+balloon+", swapped: "+swapped;
   }

   /**
    * Accessor for the power state
    * @return
    */
   public boolean getPowerState() {
      return power;
   }

   /**
    * Returns the metrics for the VM in the order specified in VCStatsProducer. As of Tues 21st May 2013 that is:
    *    cpu.usagemhz.average
    *    cpu.ready.summation
    *    mem.granted.average
    *    mem.active.average
    *    mem.vmmemctl.average
    *    mem.vmmemctltarget.average
    *    mem.compressed.average
    *    mem.swapped.average
    *    mem.swaptarget.average
    */
   public long[] getMetrics() {
      long metrics[] = new long[9];

      metrics[0] = getCpuUsage();
      metrics[1] = 0;
      metrics[2] = getMemoryUsage();
      metrics[3] = getActiveMemory();
      metrics[4] = getBalloonSize();
      metrics[5] = balloonTarget;
      metrics[6] = 0;
      metrics[7] = hostSwapped;
      metrics[8] = hostSwapped;

      return metrics;
   }
}
