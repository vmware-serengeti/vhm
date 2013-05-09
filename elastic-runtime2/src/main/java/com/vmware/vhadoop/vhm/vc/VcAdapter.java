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
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vim.binding.vim.Task;
import com.vmware.vim.vmomi.client.Client;

public class VcAdapter implements VCActions {
   private static final Logger _log = Logger.getLogger("VcAdapter");

   private Client _cloneClient;   // used for stats polling and the main waitForPropertyChange loop
   private Client _defaultClient; // used for rest of VC operations
   private VcVlsi _vcVlsi;
   private VcCredentials _vcCreds;
   private String _rootFolderName; // root folder for this VHM instance

   private ThreadLocalCompoundStatus _threadLocalStatus;

   public void setThreadLocalCompoundStatus(ThreadLocalCompoundStatus tlcs) {
      _threadLocalStatus = tlcs;
      _vcVlsi.setThreadLocalCompoundStatus(tlcs);
   }

   private CompoundStatus getCompoundStatus() {
      if (_threadLocalStatus == null) {
         if (_log.getLevel().intValue() <= Level.FINER.intValue()) {
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

   private void connect() {
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
         throw new RuntimeException("VC connection failed");
      }
   }

   // Reconnect to VC if connection timed out
   private void validateConnection() {
      if (!_vcVlsi.testConnection()) {
         _log.log(Level.WARNING, "Found VC connection dropped; reconnecting");
         connect();
      }
   }

   public VcAdapter(VcCredentials vcCreds, String rootFolderName) {
      _rootFolderName = rootFolderName;
      _vcCreds = vcCreds;
      connect();
   }

   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean powerOn) {
      validateConnection();
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
   public String waitForPropertyChange(String folderName, String version, List<VMEventData> vmDataList) {
      validateConnection();
      return _vcVlsi.waitForUpdates(_cloneClient, folderName, version, vmDataList);
   }

   @Override
   public Client getStatsPollClient() {
      return _cloneClient;
   }

   @Override
   public List<String> listVMsInFolder(String folderName) {
      validateConnection();
      return _vcVlsi.getVMsInFolder(_rootFolderName, folderName);
   }

}
