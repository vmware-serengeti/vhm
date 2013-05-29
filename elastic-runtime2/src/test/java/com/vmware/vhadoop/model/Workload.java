package com.vmware.vhadoop.model;

import com.vmware.vhadoop.model.allocation.Allocation;

/**
 * This provides the base for workloads with specific behaviour. It should be specialzed to the workload in question
 * as the base implementations provides a workload that requires no resources and exits immediately. It is likely
 * that this workload will never be scheduled due to it not asking for resource.
 * @author ghicken
 *
 */
public class Workload extends ResourceContainer
{
   long startTime = 0;
   Interval totals;

   protected Workload(String id) {
      super(id);
      totals = new Interval();
      totals.setDuration(0);
      totals.setInstructions(0);
   }

   /**
    * Stops the workload.
    * @param reason the reason for stopping this workload
    * @param force whether this is a forceful termination
    */
   public void stop(String reason, boolean force) {
      /* stopping is a no-op under default implementation */
   }

   /**
    * Before a workload can be scheduled we must know what resources it requires; that is accomplished by
    * loading it. The workload is provided with information about the maximum available resources (virtual or
    * otherwise) and declares whether the resources are sufficient to load it. If the resources available are
    * insufficient then the workload fails to load. If the workload consumes resource that can't be reused
    * (e.g. pinned memory) then that resource consumption starts at this time.
    *
    *  @param allocation the resources available to the workload
    *  @return allocation required by the workload's initial stable interval, or null if it can't run
    */
   public Allocation load(Allocation allocation) {
      return this.allocation;
   }

   /**
    * Run the workload.
    * @param allocation
    * @return
    */
   public Interval run(Allocation allocation) {
      if (startTime == 0) {
         startTime = System.currentTimeMillis();
      }

      return new Interval();
   }

   /**
    * This suspends the workload. This will be 'preemptive', however the stable interval supplied by the workload
    * will influence when this action is taken. The workload should return the desired allocation for it's next
    * period. Returning null informs the scheduler that the workload has finished and all associated resources will
    * be released.
    * Records the totals for duration and instructions that this workload's been running for. Overriding classes
    * should either call this implementation or track this for themselves.
    *
    * @param the duration of the previous interval - convenience to permit workload to easily determine subsequent allocation requirements.
    * @return the allocation desired for the workload's next period, null if the workload's finished.
    */
   public Allocation suspend(Interval interval) {
      totals.instructions+= interval.instructions;
      totals.millis+= interval.millis;

      return this.allocation;
   }

   /**
    * Gets the maximum stable interval for the container and it's children. Should be overridden by
    * inheritors who don't have stable resource usage patterns. The default is the maximum possible interval, i.e.
    * completely stable workload.
    *
    * @return the maximum guaranteed stable interval for the workload
    */
   @Override
   public Interval getStableInterval() {
      return new Interval();
   }
}
