package com.vmware.vhadoop.model;


public class VM extends ResourceContainer
{
   long balloon;
   long swapped;

   /**
    * This creates a new VM with the specifed configuration and management orchestration
    * @param orchestrator - the orchestrator that causes changes to propogate through the model
    * @param vRAM - the max memory statically configured for the VM
    * @param cpu - the max memory configured for the VM
    */
   VM(Orchestrator orchestrator, long vRAM, long cpu) {
      super(orchestrator);

      setCpuLimit(cpu);
      setMemoryLimit(vRAM);
   }

   public void execute(Workload workload) {
      add(workload);
      orchestrator.update();
   }

   /**
    * Reconfigure the VM with new vRam and CPU figures, then power on
    * @param config
    */
   public void powerOn(Limits config) {
      setCpuLimit(config.getCpuLimit());
      setMemoryLimit(config.getMemoryLimit());

      /* TODO: create a 'bootup' workload that runs immediately after poweron */
   }

   /**
    * Power on the VM using its existing configuration
    */
   public void powerOn() {
      this.powerOn(this);
   }

   /**
    * Power off the VM. This also terminates any workloads that are running
    */
   public void powerOff() {
      for (ResourceUsage usage : usages) {
         if (usage instanceof Workload) {
            Workload workload = (Workload)usage;
            workload.stop(true);
         }
      }

      usages.clear();
      orchestrator.update();
   }
}
