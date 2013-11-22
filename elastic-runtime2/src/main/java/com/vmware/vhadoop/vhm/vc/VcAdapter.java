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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ExternalizedParameters;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.vc.VcClientFactory.VcClientKey;
import com.vmware.vim.binding.vim.PerformanceManager;
import com.vmware.vim.binding.vim.Task;
import com.vmware.vim.binding.vim.event.Event.EventSeverity;
import com.vmware.vim.vmomi.client.Client;

public class VcAdapter implements VCActions {
   private static final Logger _log = Logger.getLogger(VcAdapter.class.getName());

   private final long SLEEP_RETRY_LOOKING_FOR_VALID_CLUSTERS = ExternalizedParameters.get().getLong("SLEEP_RETRY_LOOKING_FOR_VALID_CLUSTERS");   /* If no clusters are installed, we should be pretty much dormant */
   private final Long CUSTOM_RESET_CLIENT_TIMEOUT = 10000L;
   
   private final VcClientFactory _clientFactory;      /* THREADING: Thread-safe lazy access to VC clients */
   private final VcVlsi _vcVlsi;                      /* THREADING: Thread-safe singleton */
   private final String _rootFolderName;   // root folder for this VHM instance

   private ThreadLocalCompoundStatus _threadLocalStatus;

   public void setThreadLocalCompoundStatus(ThreadLocalCompoundStatus tlcs) {
      _threadLocalStatus = tlcs;
      _vcVlsi.setThreadLocalCompoundStatus(tlcs);
   }

   /* TODO: CompoundStatus is currently unused in VC functions, but we can get richer failure info if/when it's used */
   @SuppressWarnings("unused")
   private CompoundStatus getCompoundStatus() {
      if (_threadLocalStatus == null) {
         if (_log.isLoggable(Level.FINER)) {
            _log.finer("Returning dummy status compound status for thread "+Thread.currentThread());
         }
         return new CompoundStatus("DUMMY_STATUS");
      }
      return _threadLocalStatus.get();
   }

   public VcAdapter(VcCredentials vcCreds, String rootFolderName) {
      _rootFolderName = rootFolderName;
      _vcVlsi = new VcVlsi();
      _clientFactory = new VcClientFactory(_vcVlsi, vcCreds);
   }

   /* THREADING: Can be called by multiple threads */
   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean powerOn) {
      Client client = _clientFactory.getAndValidateClient(VcClientKey.CONTROL_CLIENT);
      if (client == null) {
         return null;
      }
      Map<String, Task> taskList = null;
      if (powerOn) {
         taskList = _vcVlsi.powerOnVMs(client, vmMoRefs);
      } else {
         taskList = _vcVlsi.powerOffVMs(client, vmMoRefs);
      }
      return convertTaskListToFutures(client, taskList);
   }

   private Map<String, Future<Boolean>> convertTaskListToFutures(final Client client, Map<String, Task> taskList) {
      Map<String, Future<Boolean>> result = new HashMap<String, Future<Boolean>>();
      for (String moRef : taskList.keySet()) {
         final Task task = taskList.get(moRef);
         result.put(moRef, new Future<Boolean>() {
            @Override
            public boolean cancel(boolean arg0) {
               return false;
            }

            @Override
            public Boolean get() throws InterruptedException, ExecutionException {
               return _vcVlsi.waitForTask(client, task);
            }

            @Override
            public Boolean get(long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException {
               return null;
            }

            @Override
            public boolean isCancelled() {
               return false;
            }

            @Override
            public boolean isDone() {
               return false;
            }});
      }
      return result;
   }

   /* THREADING: Single-threaded. This is only ever called by the ClusterStateChangeListener thread */
   @Override
   public List<VMEventData> waitForPropertyChange(String folderName) throws InterruptedException {
      Client client = _clientFactory.getAndValidateClient(VcClientKey.WAIT_FOR_UPDATE_CLIENT);
      if (client == null) {
         return null;
      }
      List<VMEventData> result = new ArrayList<VMEventData>();
      for (int i=0; i<2; i++) {
         String waitForUpdatesVersion = _clientFactory.getWaitForUpdatesVersion();
         String versionStatus = _vcVlsi.waitForUpdates(client, folderName, waitForUpdatesVersion, result);
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_CANCELED_STATUS)) {
            _log.fine("WaitForUpdates cancelled - throwing InterruptedException");
            throw new InterruptedException();
         } else
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_INVALID_COLLECTOR_VERSION_STATUS)) {
            client = _clientFactory.resetClient(VcClientKey.WAIT_FOR_UPDATE_CLIENT, CUSTOM_RESET_CLIENT_TIMEOUT);
            if (client != null) {
               _log.fine("Invalid property collector version - successfully reset VC client");
               continue; 
            } else {
               _log.fine("Invalid property collector version - could not reset VC client");
               break;
            }
         } else
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_INVALID_PROPERTY_STATUS)) {
            client = _clientFactory.resetClient(VcClientKey.WAIT_FOR_UPDATE_CLIENT, CUSTOM_RESET_CLIENT_TIMEOUT);
            if (client != null) {
               _log.fine("Invalid property status - successfully reset VC client");
               continue; 
            } else {
               _log.fine("Invalid property status - could not reset VC client");
               break;
            }
         } else
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_NO_CLUSTERS)) {
            /* If no clusters are yet created, VHM should be pretty much dormant */
            _log.fine("Sleeping for "+SLEEP_RETRY_LOOKING_FOR_VALID_CLUSTERS+"ms");
            Thread.sleep(SLEEP_RETRY_LOOKING_FOR_VALID_CLUSTERS);
         } else if (!waitForUpdatesVersion.equals(versionStatus)) {
            _log.fine("Updating waitForUpdates version to "+versionStatus);
            _clientFactory.updateWaitForUpdatesVersion(versionStatus);
         }
         break;
      }
      _log.finest("Returning result "+result);
      return result;
   }

   /* THREADING: Can be called by multiple threads */
   @Override
   public PerformanceManager getPerformanceManager() {
      Client client = _clientFactory.getAndValidateClient(VcClientKey.STATS_POLL_CLIENT);
      if (client == null) {
         return null;
      }
      return _vcVlsi.getPerformanceManager(client);
   }

   /* THREADING: Can be called by multiple threads */
   @Override
   public List<String> listVMsInFolder(String folderName) {
      Client client = _clientFactory.getAndValidateClient(VcClientKey.CONTROL_CLIENT);
      if (client == null) {
         return null;
      }
      return _vcVlsi.getVMsInFolder(client, _rootFolderName, folderName);
   }

   @Override
   public void interruptWait() {
      _vcVlsi.cancelWaitForUpdates();
   }

   /**
    * This logs an event with VC for the specified VM.
    *
    * @param level error, warning or info
    * @param vm the managed object reference of the VM the message applies to
    * @param message the message to display in VC and serengeti cluster detail
    * 
    * @return true if successful, false otherwise
    */
   /* THREADING: Can be called by multiple threads */
   @Override
   public boolean logEventForVM(EventSeverity level, String vmMoRef, String message) {
      Client client = _clientFactory.getAndValidateClient(VcClientKey.CONTROL_CLIENT);
      if (client == null) {
         return false;
      }
      return _vcVlsi.postEventForVM(client, vmMoRef, level, message);
   }


   @Override
   /* THREADING: Can be called by multiple threads */
   public void raiseAlarm(String vmMoRef, String message) {
      logEventForVM(EventSeverity.warning, vmMoRef, message);
   }

   @Override
   /* THREADING: Can be called by multiple threads */
   public void clearAlarm(String vmMoRef) {
      /* switch the VM back to green */
      Client client = _clientFactory.getAndValidateClient(VcClientKey.CONTROL_CLIENT);
      if (client == null) {
         return;
      }
      _vcVlsi.postEventForVM(client, vmMoRef, EventSeverity.info, "all health issues previously reported by Big Data Extensions are in remission");
      _vcVlsi.acknowledgeAlarm(client, _rootFolderName, vmMoRef);
   }
}
