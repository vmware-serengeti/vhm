package com.vmware.vhadoop.model;

import static com.vmware.vhadoop.model.Resource.MEMORY;

import com.vmware.vhadoop.model.allocation.Allocation;
import com.vmware.vhadoop.model.allocation.VirtualMemoryAllocator;

public class Host extends WorkloadExecutor
{
   Orchestrator orchestrator;

   public Host(String id, long cpu, long memory, Orchestrator orchestrator) {
      super(id);

      setCpuLimit(cpu);
      setMemoryLimit(memory);

      this.orchestrator = orchestrator;
      allocators.put(MEMORY, new VirtualMemoryAllocator(id, memory));
   }

   public void add(VM vm) {
      super.add(vm);

      vm.setHost(this);
   }

   /**
    * This powers on the specified VM
    * @param target
    * @return
    */
   public boolean powerOn(VM target) {
      Allocation required = target.load(getAvailableResources(target));
      if (required == null) {
         /* we're unable to load this VM given available resources */
         return false;
      }

      /* TODO: add the VM to the active workload set */
      return true;
   }
}
