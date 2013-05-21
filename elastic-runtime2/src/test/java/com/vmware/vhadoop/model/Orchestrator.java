package com.vmware.vhadoop.model;

public class Orchestrator
{
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
   public void update() {

   }
}
