package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.model.hadoop.JobTracker;
import com.vmware.vhadoop.vhm.model.scenarios.FaultInjectionSerengeti;
import com.vmware.vhadoop.vhm.model.scenarios.Master;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

abstract public class AbstractFaultInjectionSerengetiTestBase extends ModelTestBase<FaultInjectionSerengeti,FaultInjectionSerengeti.FaultInjectionMaster,JobTracker>
{
   @Override
   protected FaultInjectionSerengeti createSerengeti(String name, VirtualCenter vCenter) {
      return new FaultInjectionSerengeti(name, vCenter);
   }

   @Override
   protected Master.Template getMasterTemplate() {
      return new FaultInjectionSerengeti.MasterTemplate();
   }


   /**
    * Returns the JobTracker for the cluster
    */
   @Override
   protected JobTracker getApplication(FaultInjectionSerengeti.FaultInjectionMaster master) {
      String port = master.getExtraInfo().get("vhmInfo.jobtracker.port");
      return (JobTracker)master.getOS().connect(port);
   }
}
