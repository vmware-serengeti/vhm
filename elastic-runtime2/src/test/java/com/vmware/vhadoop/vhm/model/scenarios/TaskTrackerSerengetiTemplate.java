/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

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
