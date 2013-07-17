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
