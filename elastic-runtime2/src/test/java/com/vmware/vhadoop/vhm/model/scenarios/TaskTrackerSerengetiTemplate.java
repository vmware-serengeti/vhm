package com.vmware.vhadoop.vhm.model.scenarios;

import com.vmware.vhadoop.vhm.model.hadoop.HadoopJob;
import com.vmware.vhadoop.vhm.model.hadoop.TaskTracker;

public class TaskTrackerSerengetiTemplate extends Compute.Template
{
   TaskTrackerSerengetiTemplate(Master master) {
      super(master);
   }

   @Override
   protected void specialize(Compute compute, Serengeti serengeti) {
      super.specialize(compute, serengeti);

      String port = master.getExtraInfo().get("vhmInfo.jobtracker.port");
      String host = master.getHostname();
      TaskTracker tracker = new TaskTracker("tasktracker-"+compute.getHostname(), host, port);
      compute.getOS().install(tracker);
      tracker.setSlots(HadoopJob.SlotType.MAP, 1);
      tracker.setSlots(HadoopJob.SlotType.REDUCE, 1);
   }
}
