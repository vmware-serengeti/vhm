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

package com.vmware.vhadoop.vhm.vc;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.model.api.Workload;
import com.vmware.vhadoop.vhm.model.vcenter.Folder;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vim.binding.vim.PerformanceManager;

public class ModelVcAdapter implements VCActions {
   private static final Logger _log = Logger.getLogger("VcModelAdapter");

   private ThreadLocalCompoundStatus _threadLocalStatus;
   private VirtualCenter _vCenter;
   private Thread _updateThread;

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


   /**
    * This method is expected to block until there are applicable updates
    */
   @Override
   public List<VMEventData> waitForPropertyChange(String folderName) throws InterruptedException {
      List<VM> vms;
      try {
         setUpdateThread(Thread.currentThread());
         vms = _vCenter.getUpdatedVMs();
      } finally {
         setUpdateThread(null);
      }

      List<VMEventData> result = new ArrayList<VMEventData>();
      for (VM vm : vms) {
         /* build the VMEventData list */
         VMEventData vmData = new VMEventData();
         vmData._vmMoRef = vm.getId();
         vmData._dnsName = vm.getHostname();
         vmData._hostMoRef = vm.getHost() != null ? vm.getHost().getId() : null;
         vmData._ipAddr = vm.getIpAddress();
         vmData._isLeaving = false;
         vmData._myName = vm.getId();
         vmData._myUUID = vm.getId();
         vmData._powerState = vm.powerState();
         vmData._vCPUs = 1;

         /* parse out the extraInfo fields into the event */
         Map<String,String> extraInfo = vm.getExtraInfo();
         for (String key : extraInfo.keySet()) {
            String value = extraInfo.get(key);
            VcVlsiHelper.parseExtraConfig(vmData, key, value);
         }

         result.add(vmData);
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
   public synchronized void interruptWait() {
      if (_updateThread != null) {
         _updateThread.interrupt();
      }
   }

   private synchronized void setUpdateThread(Thread thread) {
      _updateThread = thread;
   }

   @Override
   public PerformanceManager getPerformanceManager() {
      // TODO Auto-generated method stub
      return null;
   }
}
