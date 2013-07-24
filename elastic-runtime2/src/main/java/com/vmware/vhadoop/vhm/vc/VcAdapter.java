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
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vim.binding.vim.Task;
import com.vmware.vim.vmomi.client.Client;

public class VcAdapter implements VCActions {
   private static final Logger _log = Logger.getLogger(VcAdapter.class.getName());

   private Client _cloneClient;   // used for stats polling and the main waitForPropertyChange loop
   private Client _defaultClient; // used for rest of VC operations
   private VcVlsi _vcVlsi;
   private final VcCredentials _vcCreds;
   private final String _rootFolderName; // root folder for this VHM instance

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

   // returns true if it successfully connected to VC
   private boolean initClients(boolean useCert) {
      try {
         _defaultClient = _vcVlsi.connect(_vcCreds, useCert, false);
         _cloneClient = _vcVlsi.connect(_vcCreds, useCert, true);
         if ((_defaultClient == null) || (_cloneClient == null)) {
            _log.log(Level.WARNING, "Unable to get VC client");
            return false;
         }
         return true;
      } catch (Exception e) {
         _log.log(Level.WARNING, "VC connection failed ("+e.getClass()+"): "+e.getMessage());
         return false;
      }
   }

   private boolean connect() {
      _vcVlsi = new VcVlsi();

      boolean useCert = false;
      if ((_vcCreds.keyStoreFile != null) && (_vcCreds.keyStorePwd != null) && (_vcCreds.vcExtKey != null)) {
         useCert = true;
      }
      boolean success = initClients(useCert);
      if (useCert && !success && (_vcCreds.user != null) && (_vcCreds.password != null)) {
         _log.log(Level.WARNING, "Cert based login failed, trying user/password");
         success = initClients(false);
      }
      if (!success) {
         _log.warning("Could not obtain VC connection through any protocol");
         return false;
      }
      return true;
   }

   /*
    *  Reconnect to VC if connection timed out on either session
    *  Checking both session will also have the side effect of keep both sessions active whenever validate is called. 
    */
   private boolean validateConnection() {
      boolean success = _vcVlsi.testConnection(_defaultClient) && _vcVlsi.testConnection(_cloneClient);
      if (!success) {
         _log.log(Level.WARNING, "Found VC connection dropped; reconnecting");
         return connect();
      }
      return success;
   }

   public VcAdapter(VcCredentials vcCreds, String rootFolderName) {
      _rootFolderName = rootFolderName;
      _vcCreds = vcCreds;
      if (!connect()) {
         _log.warning("Could not initialize connection to VC");
      }
   }

   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean powerOn) {
      if (!validateConnection()) {
         return null;
      }
      Map<String, Task> taskList = null;
      if (powerOn) {
         taskList = _vcVlsi.powerOnVMs(vmMoRefs);
      } else {
         taskList = _vcVlsi.powerOffVMs(vmMoRefs);
      }
      return convertTaskListToFutures(taskList);
   }

   private Map<String, Future<Boolean>> convertTaskListToFutures(Map<String, Task> taskList) {
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
               return _vcVlsi.waitForTask(task);
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

   @Override
   public String waitForPropertyChange(String folderName, String version, List<VMEventData> vmDataList) throws InterruptedException {
      if (!validateConnection()) {
         return null;
      }
      String result = _vcVlsi.waitForUpdates(_cloneClient, folderName, version, vmDataList);
      if (result.equals(VcVlsi.WAIT_FOR_UPDATES_CANCELED_STATUS)) {
         throw new InterruptedException();
      }
      return result;
   }

   @Override
   public Client getStatsPollClient() {
      return _cloneClient;
   }

   @Override
   public List<String> listVMsInFolder(String folderName) {
      if (!validateConnection()) {
         return null;
      }
      return _vcVlsi.getVMsInFolder(_rootFolderName, folderName);
   }

   @Override
   public void interruptWait() {
      _vcVlsi.cancelWaitForUpdates();
   }

}
