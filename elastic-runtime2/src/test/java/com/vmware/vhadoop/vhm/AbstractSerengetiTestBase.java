package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.model.scenarios.Serengeti;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

abstract public class AbstractSerengetiTestBase extends ModelTestBase<Serengeti,Serengeti.Master>
{
   @Override
   protected Serengeti createSerengeti(String name, VirtualCenter vCenter) {
      return new Serengeti(name, vCenter);
   }
}
