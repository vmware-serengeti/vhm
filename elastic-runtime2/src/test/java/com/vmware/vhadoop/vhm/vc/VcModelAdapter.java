package com.vmware.vhadoop.vhm.vc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.model.Orchestrator;
import com.vmware.vhadoop.model.VM;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vim.vmomi.client.Client;

public class VcModelAdapter implements VCActions {
   private static final Logger _log = Logger.getLogger("VcModelAdapter");

   private ThreadLocalCompoundStatus _threadLocalStatus;
   private Orchestrator _orchestrator;

   public VcModelAdapter(Orchestrator orchestrator) {
      _orchestrator = orchestrator;
   }

   public void setThreadLocalCompoundStatus(ThreadLocalCompoundStatus tlcs) {
      _threadLocalStatus = tlcs;
   }

   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean powerOn) {
      Map<String, Future<Boolean>> taskList = null;
      if (powerOn) {
         taskList = _orchestrator.powerOnVMs(vmMoRefs);
      } else {
         taskList = _orchestrator.powerOffVMs(vmMoRefs);
      }

      return taskList;
   }

   /**
    * This method is expected to block until there are applicable updates
    */
   @Override
   public String waitForPropertyChange(String folderName, String version, List<VMEventData> vmDataList) throws InterruptedException {
      String result = version;

      if (!version.equals("")) {
         _log.severe("Version is presumed to be \"\", version specified was "+version);
         return "";
      }

      List<VM> vms = _orchestrator.getUpdatedVMs();
      for (VM vm : vms) {
         /* build the VMEventData list */
         VMEventData vmData = new VMEventData();
         vmData._vmMoRef = vm.getId();
         vmData._dnsName = "";
         vmData._hostMoRef = "";
         vmData._ipAddr = "";
         vmData._isElastic = true;
         vmData._isLeaving = false;
         vmData._masterMoRef = "";
         vmData._masterUUID = "";
         vmData._masterVmData = null;
         vmData._myName = "";
         vmData._myUUID = "";
         vmData._powerState = vm.getPowerState();
         vmData._serengetiFolder = "";
         vmData._vCPUs = (int) (vm.getCpuLimit() / _orchestrator.getCpuSpeed());

         vmDataList.add(vmData);
      }

      /* TODO: figure out whether this value is ever actually returned by VC and when we should return it */
      if (result.equals("Some special value")) {
         throw new InterruptedException();
      }
      return result;
   }


   @Override
   public List<String> listVMsInFolder(String folderName) {
      return null;
   }

   @Override
   public void interruptWait() {
      /* noop */
   }

   @Override
   public Client getStatsPollClient() {
      /* TODO: return a stats accessor for the model */
      return null;
   }
}
