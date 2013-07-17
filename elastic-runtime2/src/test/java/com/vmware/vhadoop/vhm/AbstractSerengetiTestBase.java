package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.model.hadoop.JobTracker;
import com.vmware.vhadoop.vhm.model.scenarios.JobTrackerSerengetiTemplate;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti;
import com.vmware.vhadoop.vhm.model.scenarios.Master;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

abstract public class AbstractSerengetiTestBase extends ModelTestBase<Serengeti,Master,JobTracker>
{
   @Override
   protected Serengeti createSerengeti(String name, VirtualCenter vCenter) {
      return new Serengeti(name, vCenter);
   }

   @Override
   protected Master.Template getMasterTemplate() {
      return new JobTrackerSerengetiTemplate();
   }

   /**
    * Returns the JobTracker for the cluster
    */
   @Override
   protected JobTracker getApplication(Master master) {
      String port = master.getExtraInfo().get("vhmInfo.jobtracker.port");
      return (JobTracker)master.getOS().connect(port);
   }
}
