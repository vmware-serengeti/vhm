package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.model.scenarios.FaultInjectionSerengeti;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

abstract public class AbstractFaultInjectionSerengetiTestBase extends ModelTestBase<FaultInjectionSerengeti,FaultInjectionSerengeti.Master>
{
   @Override
   protected FaultInjectionSerengeti createSerengeti(String name, VirtualCenter vCenter) {
      return new FaultInjectionSerengeti(name, vCenter);
   }


}
