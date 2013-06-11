package com.vmware.vhadoop.vhm.model.scenarios;

import com.vmware.vhadoop.vhm.model.Allocation;
import com.vmware.vhadoop.vhm.model.api.ResourceType;
import com.vmware.vhadoop.vhm.model.os.Linux;
import com.vmware.vhadoop.vhm.model.vcenter.Host;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

public class BasicScenario
{
   public static final Allocation DEFAULT_HOST_CAPACITY = Allocation.zeroed();
   public static final Allocation DEFAULT_VM_CAPACITY = Allocation.zeroed();

   static {
      /* 8x2Ghz cpus, 24Gb memory */
      DEFAULT_HOST_CAPACITY.set(ResourceType.CPU, 16000);
      DEFAULT_HOST_CAPACITY.set(ResourceType.MEMORY, 24 * 1024);

      /* 2x2Ghz cpus, 6Gb Memory */
      DEFAULT_VM_CAPACITY.set(ResourceType.CPU, 4000);
      DEFAULT_VM_CAPACITY.set(ResourceType.MEMORY, 6 * 1024);
   }


   /**
    * Constructs a symmetrical scenario with a given number of hosts and VMs per host with default capacities
    * @param hosts - number of hosts
    * @param vms - vms per host
    */
   public static VirtualCenter getVCenter(int hosts, int vms) {
      return getVCenter(hosts, DEFAULT_HOST_CAPACITY, vms, DEFAULT_VM_CAPACITY);
   }

   public static VirtualCenter getVCenter(int hosts, com.vmware.vhadoop.vhm.model.api.Allocation hostCapacity) {
      return getVCenter(hosts, hostCapacity, 0, DEFAULT_VM_CAPACITY);
   }

   public static VirtualCenter getVCenter(int hosts, com.vmware.vhadoop.vhm.model.api.Allocation hostCapacity, int vms) {
      return getVCenter(hosts, hostCapacity, vms, DEFAULT_VM_CAPACITY);
   }

   public static VirtualCenter getVCenter(int hosts, int vms, com.vmware.vhadoop.vhm.model.api.Allocation vmCapacity) {
      return getVCenter(hosts, DEFAULT_HOST_CAPACITY, vms, vmCapacity);
   }

   public static VirtualCenter getVCenter(int hosts, com.vmware.vhadoop.vhm.model.api.Allocation hostCapacity, int vms, com.vmware.vhadoop.vhm.model.api.Allocation vmCapacity) {
      VirtualCenter vCenter = new VirtualCenter("BasicScenario");

      for (int i = 0; i < hosts; i++) {
         Host host = vCenter.createHost("host"+i, hostCapacity);
         host.powerOn();

         /* 2x2Ghz cpus, 6Gb Memory */
         for (int j = 0; j < vms; j++) {
            VM vm = vCenter.createVM("vm"+((i*vms)+j), vmCapacity);
            Linux linux = new Linux("linux");
            vm.install(linux);
            host.add(vm);
         }
      }

      return vCenter;
   }
}
