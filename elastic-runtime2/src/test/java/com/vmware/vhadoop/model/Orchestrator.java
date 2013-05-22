package com.vmware.vhadoop.model;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Top level container for various other entities, such as hosts, vms, resource pools, et al.
 * @author ghicken
 *
 */
public class Orchestrator extends ResourceContainer
{
   /** Nominal CPU speed is 2GHz */
   long cpuSpeed = 2000;

   /** List of VMs that have been updated since the last time the list was retrieved */
   List<VM> updates;

   public Orchestrator(String id) {
      super(id);
      updates = new LinkedList<VM>();
   }

   AllocationPolicy allocationPolicy;

   /**
    * This returns the allocation policy used to portion out scarce resources.
    * This accessor is provided so that subtrees of containers can use it when they determine that there's
    * a need rather than processing the entire hierarchy every time.
    * @return
    */
   public AllocationPolicy getAllocationPolicy() {
      return allocationPolicy;
   }

   /**
    * This causes a global re-evaluation of resource usage patterns against limits.
    */
   @Override
   public void update() {

   }

   private Map<String, Future<Boolean>> setPower(Set<String> ids, boolean power) {
      Map<String, Future<Boolean>> map = new HashMap<String, Future<Boolean>>();

      for (String id : ids) {
         VM vm = (VM)get(id);
         /* TODO: alter this if we ever model power on to take time */
         final boolean result = power ? vm.powerOn() : vm.powerOff();
         map.put(id, new Future<Boolean>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
               return false;
            }

            @Override
            public boolean isCancelled() {
               return false;
            }

            @Override
            public boolean isDone() {
               return true;
            }

            @Override
            public Boolean get() throws InterruptedException, ExecutionException {
               return result;
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
               return result;
            }});
      }

      return map;
   }

   public Map<String, Future<Boolean>> powerOnVMs(Set<String> ids) {
      return setPower(ids, true);
   }

   public Map<String, Future<Boolean>> powerOffVMs(Set<String> ids) {
      return setPower(ids, false);
   }

   @SuppressWarnings("unchecked")
   public List<VM> getUpdatedVMs() {
//    List<VM> vms = this.updates;
//    this.updates = new LinkedList<VM>();
//
//    return vms;

      /* TODO: make this a little more fine grained */
      return (List<VM>)get(VM.class);
   }

   /**
    * Sets the nominal speed of CPUs in this orchestration. Limits are specified in Mhz, so
    * this is a mechanism of converting from Mhz allocation to vCPU count.
    * @param Mhz
    */
   public void setCpuSpeed(long Mhz) {
      this.cpuSpeed = Mhz;
   }

   /**
    * Gets the nominal speed of CPUs in this orchestration. Limits are specified in Mhz, so
    * this is a mechanism of converting from Mhz allocation to vCPU count.
    * @return Mhz
    */
   public long getCpuSpeed() {
      return this.cpuSpeed;

   }

   /**
    * This returns an array of metric values in the order determined by the vcStatsList in VCStatsProducer
    * @param id the id of the VM to provide metrics for
    * @return the metric values
    */
   public long[] getMetrics(String id) {
      VM vm = (VM)get(id);
      return vm.getMetrics();
   }
}
