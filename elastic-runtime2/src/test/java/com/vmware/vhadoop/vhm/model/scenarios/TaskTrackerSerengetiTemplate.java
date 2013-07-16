package com.vmware.vhadoop.vhm.model.scenarios;

import com.vmware.vhadoop.vhm.model.hadoop.HadoopJob;
import com.vmware.vhadoop.vhm.model.hadoop.TaskTracker;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.Compute;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.ComputeTemplate;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.Master;

public class TaskTrackerSerengetiTemplate extends ComputeTemplate
{
   @Override
   protected void specialize(Compute compute, Master master) {
      super.specialize(compute, master);

      String port = master.getExtraInfo().get("vhmInfo.jobtracker.port");
      String host = master.getHostname();
      TaskTracker tracker = new TaskTracker(compute.getHostname(), host, port);
      compute.getOS().install(tracker);
      tracker.setSlots(HadoopJob.SlotType.MAP, 1);
      tracker.setSlots(HadoopJob.SlotType.REDUCE, 1);
   }
}
