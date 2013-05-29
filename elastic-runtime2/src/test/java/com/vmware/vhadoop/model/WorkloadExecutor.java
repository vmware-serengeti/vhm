package com.vmware.vhadoop.model;

import com.vmware.vhadoop.model.allocation.Allocation;

public class WorkloadExecutor extends Workload
{
   protected WorkloadExecutor(String id) {
      super(id);
   }

   public void execute(Workload workload) {
      Allocation available = getAvailableResources(workload);
      Allocation requested = workload.load(available);
      if (requested != null) {
         /* will allocate the persistent portions of the request (i.e. pinned) */
         allocate(workload.id, requested);
         add(workload);
      }
   }


   @Override
   public void stop(String reason, boolean force) {
      /* TODO: call through to host to make sure it knows this VM's powered off */
      super.stop(reason, force);
      allocation = null;
   }

   /**
    * Tells the VM that it's now active with the specified resources
    */
   @Override
   public Interval run(Allocation allocation) {
      super.run(allocation);

      Interval interval = getStableInterval();

      for (ResourceContainer container : children) {
         if (container instanceof Workload) {
            Workload workload = (Workload)container;
            /* given all of the parents an opportunity to modify this allocation */
            Allocation approved = allocation.minimums(getAvailableResources(this));
            approved = allocate(workload.getId(), approved);
            Interval i = workload.run(approved);
            interval = interval.minimum(i);
         }
      }

      return interval;
   }

   /**
    * VM currently has no direct overhead while not running a workload.
    */
   @Override
   public Allocation load(Allocation available) {
      allocation = new Allocation(id);
      return super.load(available);
   }

   /**
    * Builds a composite allocation from the workload's hosted within this VM that are scheduled to run. If they
    * require more resources than the current allocation, but still within the container limits, then request more,
    * otherwise use the previous allocation and deal with any internal overcommit.
    */
   @Override
   public Allocation suspend(Interval interval) {
      super.suspend(interval);

      Allocation sum = new Allocation(id);
      for (ResourceContainer container : children) {
         if (container instanceof Workload) {
            Workload workload = (Workload)container;
            sum = sum.add(workload.suspend(interval));
         }
      }

      /* enforce the limits of this container */
      sum = sum.minimums(limits);

      return sum;
   }
}
