package com.vmware.vhadoop.model.scenarios;

import com.vmware.vhadoop.vhm.model.Allocation;
import com.vmware.vhadoop.vhm.model.api.ResourceType;
import com.vmware.vhadoop.vhm.model.os.Linux;
import com.vmware.vhadoop.vhm.model.vcenter.Host;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

public class BasicScenario
{

   /**
    * Constructs a symmetrical scenario with a given number of hosts and VMs per host
    * @param hosts - number of hosts
    * @param vms - vms per host
    */
   public static VirtualCenter getVCenter(int hosts, int vms) {
      VirtualCenter vCenter = new VirtualCenter("BasicScenario");

      for (int i = 0; i < hosts; i++) {
         /* 8x2Ghz cpus, 24Gb memory */
         Allocation capacity = Allocation.zeroed();
         capacity.set(ResourceType.CPU, 16000);
         capacity.set(ResourceType.MEMORY, 24 * 1024);

         Host host = vCenter.createHost("host"+i, capacity);
         host.powerOn();

         /* 2x2Ghz cpus, 6Gb Memory */
         for (int j = 0; j < vms; j++) {
            capacity = Allocation.zeroed();
            capacity.set(ResourceType.CPU, 4000);
            capacity.set(ResourceType.MEMORY, 6 * 1024);

            VM vm = vCenter.createVM("vm"+((i*vms)+j), capacity);
            Linux linux = new Linux("linux");
            vm.install(linux);
            host.add(vm);
         }
      }

      return vCenter;
   }
}
