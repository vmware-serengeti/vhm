package com.vmware.vhadoop.model;

import static com.vmware.vhadoop.model.Resource.MEMORY;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.vmware.vhadoop.model.allocation.VirtualMemoryAllocator;


public class VM extends WorkloadExecutor
{
   Orchestrator orchestrator;
   Host host;
   Map<String,String> extraInfo;

   boolean power = false;

   /**
    * This creates a new VM with the specifed configuration and management orchestration
    * @param orchestrator - the orchestrator that causes changes to propogate through the model
    * @param vRAM - the max memory statically configured for the VM
    * @param cpu - the max memory configured for the VM
    */
   protected VM(String id, long cpu, long vRAM, Orchestrator orchestrator) {
      super(id);

      this.orchestrator = orchestrator;

      setCpuLimit(cpu);
      setMemoryLimit(vRAM);

      allocators.put(MEMORY, new VirtualMemoryAllocator(id, vRAM));

      extraInfo = new HashMap<String,String>();
      orchestrator.configurationUpdated(this);
   }

   /**
    * Reconfigure the VM with new vRam and CPU figures, then power on
    * @param config
    */
   public boolean powerOn(Limits config) {
      /* TODO: if we ever model this as taking time, we need to update the Futures in Orchestrator */
      if (getHost() == null) {
         /* we can't power on a VM without a host set */
         return false;
      }

      /* if we already have an allocation we don't need to proxy the power on to the host */
      if (allocation == null) {
         if (!getHost().powerOn(this)) {
            /* the host couldn't power this VM on */
            return false;
         }
      }

      power = true;

      setCpuLimit(config.getCpuLimit());
      setMemoryLimit(config.getMemoryLimit());

      orchestrator.configurationUpdated(this);

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
      for (ResourceContainer container : children) {
         if (container instanceof Workload) {
            Workload workload = (Workload)container;
            workload.stop("Powering off VM", true);
         }
      }

      children.clear();

      /* TODO: if we ever model this as taking time, we need to update the Futures in Orchestrator */
      power = false;

      orchestrator.configurationUpdated(this);
      return true;
   }


   /**
    * Accessor for the power state
    * @return
    */
   public boolean getPowerState() {
      return power;
   }

   @Override
   public long getMemoryUsage() {
      return allocation.getResourceUsage(MEMORY, "total");
   }

   /**
    * VM basic toString description that includes power state, balloon and swap sizes
    */
   @Override
   public String toString() {
      return super.toString() + ", power: "+(power ? "on" : "off");
   }

   /**
    * This doesn't bother cleaning up, just discards everything
    */
   @Override
   public void stop(String reason, boolean force) {
      super.stop(reason, force);

      children.clear();
      orchestrator.configurationUpdated(this);
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

      /* most of the memory stats seem to be consumed in kB */
      metrics[0] = getCpuUsage();
      metrics[1] = 0;
      metrics[2] = getMemoryUsage() * 1024;
      metrics[3] = getActiveMemory() * 1024;
//      metrics[4] = getBalloonSize() * 1024;
//      metrics[5] = balloonTarget * 1024;
//      metrics[6] = 0;
//      metrics[7] = hostSwapped * 1024;
//      metrics[8] = hostSwapped * 1024;

      return metrics;
   }

   public void setHost(Host host) {
      orchestrator.configurationUpdated(this);
      this.host = host;
   }

   public Host getHost() {
      return this.host;
   }

   /**
    * Sets an entry in the extraInfo. This will tell the orchestrator that it's configuration has been updated.
    * @param key
    * @param value
    * @return the previous value if any
    */
   public String setExtraInfo(String key, String value) {
      orchestrator.configurationUpdated(this);
      return extraInfo.put(key, value);
   }

   /**
    * Gets a readonly map that holds extra info associated with the VM
    * @return
    */
   public Map<String,String> getExtraInfo() {
      /* TODO: should this be in VM or ResourceContainer */
      return Collections.unmodifiableMap(extraInfo);
   }
}
