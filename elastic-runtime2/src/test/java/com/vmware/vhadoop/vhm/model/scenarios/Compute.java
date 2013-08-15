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


import java.util.logging.Logger;

import com.vmware.vhadoop.vhm.model.api.Allocation;
import com.vmware.vhadoop.vhm.model.os.Linux;
import com.vmware.vhadoop.vhm.model.os.Process;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;


public class Compute extends VM
{
   public static class Template extends Serengeti.Template<Compute> {
      Master master;

      Template(Master master) {
         this.master = master;
      }

      @Override
      protected Compute instantiate(VirtualCenter vCenter, String id, Allocation capacity, Serengeti serengeti) {
         return new Compute(vCenter, id, capacity, master);
      }

      @Override
      protected void configure(Compute compute, Serengeti serengeti) {
         super.configure(compute, serengeti);
         compute.install(new Linux("Linux-"+compute.name()));
         compute.setHostname(Serengeti.constructHostnameForCompute(master, compute.name()));
      }
   }

   private static Logger _log = Logger.getLogger(Compute.class.getName());

   Master master;

   Compute(VirtualCenter vCenter, String id, Allocation capacity, Master master) {
      super(vCenter, id, capacity);
      this.master = master;
      setExtraInfo("vhmInfo.elastic", "true");
      setExtraInfo("vhmInfo.masterVM.uuid", master.getClusterId());
      setExtraInfo("vhmInfo.masterVM.moid", master.getId());

      _log.info(master.getClusterId()+": created cluster compute node ("+id+")");
   }

   public void execute(Process process) {
      _log.info(name()+": executing process "+process.name());
      getOS().exec(process);
   }
}
