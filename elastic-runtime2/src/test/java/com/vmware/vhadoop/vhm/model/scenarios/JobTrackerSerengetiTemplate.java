package com.vmware.vhadoop.vhm.model.scenarios;

import com.vmware.vhadoop.vhm.model.hadoop.JobTracker;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.Master;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.MasterTemplate;

public class JobTrackerSerengetiTemplate extends MasterTemplate
{
   @Override
   protected void specialize(Master master, Serengeti serengeti) {
      super.specialize(master, serengeti);

      String port = "8080";
      master.setExtraInfo("vhmInfo.jobtracker.port", port);
      master.getOS().install(new JobTracker(master.clusterName+"-jobtracker", port));
      master.setComputeNodeTemplate(new TaskTrackerSerengetiTemplate());
   }
}
