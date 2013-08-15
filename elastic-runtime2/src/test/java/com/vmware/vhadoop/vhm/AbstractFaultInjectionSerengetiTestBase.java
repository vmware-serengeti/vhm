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

   @Override
   protected int numberComputeNodesInState(FaultInjectionSerengeti.FaultInjectionMaster master, boolean state) {
      JobTracker tracker = getApplication(master);
      if (tracker != null) {
         int active = tracker.getAliveTaskTrackers().size();
         return state ? active : master.availableComputeNodes() - active;
      }

      /* if we don't have a job tracker then all of them are "inactive" */
      return state ? 0 : master.availableComputeNodes();
   }
}
