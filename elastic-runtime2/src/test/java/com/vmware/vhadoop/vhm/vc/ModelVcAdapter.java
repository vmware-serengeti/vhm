package com.vmware.vhadoop.vhm.vc;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.model.api.ResourceType;
import com.vmware.vhadoop.vhm.model.api.Workload;
import com.vmware.vhadoop.vhm.model.vcenter.Folder;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vim.vmomi.client.Client;

public class ModelVcAdapter implements VCActions {
   private static final Logger _log = Logger.getLogger("VcModelAdapter");

   private ThreadLocalCompoundStatus _threadLocalStatus;
   private VirtualCenter _vCenter;

   public ModelVcAdapter(VirtualCenter vCenter) {
      _vCenter = vCenter;
   }

   public void setThreadLocalCompoundStatus(ThreadLocalCompoundStatus tlcs) {
      _threadLocalStatus = tlcs;
   }

   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean powerOn) {
      Map<String, Future<Boolean>> taskList = null;
      if (powerOn) {
         taskList = _vCenter.powerOnByID(vmMoRefs);
      } else {
         taskList = _vCenter.powerOffByID(vmMoRefs);
      }

      return taskList;
   }


   private static final String VHM_EXTRA_CONFIG_PREFIX = "vhmInfo.";
   private static final String VHM_EXTRA_CONFIG_UUID = "vhmInfo.serengeti.uuid";
   private static final String VHM_EXTRA_CONFIG_MASTER_UUID = "vhmInfo.masterVM.uuid";
   private static final String VHM_EXTRA_CONFIG_MASTER_MOREF = "vhmInfo.masterVM.moid";
   private static final String VHM_EXTRA_CONFIG_ELASTIC = "vhmInfo.elastic";
   private static final String VHM_EXTRA_CONFIG_AUTOMATION_ENABLE = "vhmInfo.vhm.enable";
   private static final String VHM_EXTRA_CONFIG_AUTOMATION_MIN_INSTANCES = "vhmInfo.min.computeNodeNum";
   private static final String VHM_EXTRA_CONFIG_JOB_TRACKER_PORT = "vhmInfo.jobtracker.port";

   static private MasterVmEventData getMasterVmData(VMEventData vmData) {
      if (vmData._masterVmData == null) {
         vmData._masterVmData = new MasterVmEventData();
      }
      return vmData._masterVmData;
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

      List<VM> vms = _vCenter.getUpdatedVMs();
      for (VM vm : vms) {
         /* build the VMEventData list */
         VMEventData vmData = new VMEventData();
         vmData._vmMoRef = vm.getId();
         vmData._dnsName = vm.getHostname();
         vmData._hostMoRef = vm.getHost() != null ? vm.getHost().getId() : null;
         vmData._ipAddr = "127.0.0.1";
         vmData._isLeaving = false;
         vmData._myName = vm.getId();
         vmData._myUUID = vm.getId();
         vmData._powerState = vm.powerState();
         vmData._isMaster = vmData._myName.contains("-master");
         if (vm.getMaximum() != null) {
            vmData._vCPUs = (int) (vm.getMaximum().get(ResourceType.CPU) / _vCenter.getCpuSpeed());
         } else {
            /* assume 1 if unset - could be because this VM's not assigned a host yet */
            vmData._vCPUs = 1;
         }

         /* parse out the extraInfo fields into the event */
         Map<String,String> extraInfo = vm.getExtraInfo();
         for (String key : extraInfo.keySet()) {
            String value = extraInfo.get(key);
//            VcVlsi.parseExtraConfig(vmData, key, value);

            if (key.startsWith(VHM_EXTRA_CONFIG_PREFIX) && value != null) {
               //_log.log(Level.INFO, "PEC key:val = " + key + " : " + value);
               if (key.equals(VHM_EXTRA_CONFIG_UUID)) {
                  vmData._serengetiFolder = value;
               } else if (key.equals(VHM_EXTRA_CONFIG_MASTER_UUID)) {
                  vmData._masterUUID = value;
               } else if (key.equals(VHM_EXTRA_CONFIG_MASTER_MOREF)) {
                  vmData._masterMoRef = value;
               } else if (key.equals(VHM_EXTRA_CONFIG_ELASTIC)) {
                  vmData._isElastic = value.equalsIgnoreCase("true");
               } else if (key.equals(VHM_EXTRA_CONFIG_AUTOMATION_ENABLE)) {
                  getMasterVmData(vmData)._enableAutomation = value.equalsIgnoreCase("true");
               } else if (key.equals(VHM_EXTRA_CONFIG_AUTOMATION_MIN_INSTANCES)) {
                  getMasterVmData(vmData)._minInstances = Integer.valueOf(value);
               } else if (key.equals(VHM_EXTRA_CONFIG_JOB_TRACKER_PORT)) {
                  getMasterVmData(vmData)._jobTrackerPort = Integer.valueOf(value);
               }
            }
         }

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
      Folder container = (Folder)_vCenter.get(folderName);
      List<String> ids = null;
      if (container != null) {
         ids = new LinkedList<String>();
         List<? extends Workload> workloads = container.get(VM.class);
         if (workloads != null) {
            @SuppressWarnings("unchecked")
            List<VM> vms = (List<VM>)workloads;
            for (VM vm : vms) {
               ids.add(vm.getId());
            }
         }
      }

      return ids;
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
