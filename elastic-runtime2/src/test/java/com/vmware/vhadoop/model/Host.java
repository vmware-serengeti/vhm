package com.vmware.vhadoop.model;

public class Host extends ResourceContainer
{

   public Host(String id, long cpu, long memory, Orchestrator orchestrator) {
      super(id, orchestrator);

      setCpuLimit(cpu);
      setMemoryLimit(memory);
   }
}
