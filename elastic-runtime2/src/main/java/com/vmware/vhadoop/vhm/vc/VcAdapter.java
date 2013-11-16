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
import java.util.Calendar;
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
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vim.binding.impl.vim.event.EventExImpl;
import com.vmware.vim.binding.impl.vmodl.TypeNameImpl;
import com.vmware.vim.binding.vim.ExtensionManager;
import com.vmware.vim.binding.vim.Folder;
import com.vmware.vim.binding.vim.PerformanceManager;
import com.vmware.vim.binding.vim.Task;
import com.vmware.vim.binding.vim.alarm.Alarm;
import com.vmware.vim.binding.vim.alarm.AlarmManager;
import com.vmware.vim.binding.vim.event.Event.EventSeverity;
import com.vmware.vim.binding.vim.event.EventEx;
import com.vmware.vim.binding.vim.event.EventManager;
import com.vmware.vim.binding.vim.fault.InvalidEvent;
import com.vmware.vim.binding.vmodl.ManagedObject;
import com.vmware.vim.binding.vmodl.ManagedObjectReference;
import com.vmware.vim.binding.vmodl.query.InvalidProperty;
import com.vmware.vim.vmomi.client.Client;

public class VcAdapter implements VCActions {
   private static final Logger _log = Logger.getLogger(VcAdapter.class.getName());

   private final long VC_CONTROL_CONNECTION_TIMEOUT_MILLIS = ExternalizedParameters.get().getLong("VC_CONTROL_CONNECTION_TIMEOUT_MILLIS");
   private final long VC_WAIT_FOR_UPDATES_CONNECTION_TIMEOUT_MILLIS = ExternalizedParameters.get().getLong("VC_WAIT_FOR_UPDATES_CONNECTION_TIMEOUT_MILLIS");  /* WaitForUpdates will block for at most this period */
   private final long VC_STATS_POLL_CONNECTION_TIMEOUT_MILLIS = ExternalizedParameters.get().getLong("VC_STATS_POLL_CONNECTION_TIMEOUT_MILLIS");   /* Stats collection timeout should be short */
   private final long SLEEP_RETRY_LOOKING_FOR_VALID_CLUSTERS = ExternalizedParameters.get().getLong("SLEEP_RETRY_LOOKING_FOR_VALID_CLUSTERS");   /* If no clusters are installed, we should be pretty much dormant */
   private final String VC_ALARM_NAME_BASE = ExternalizedParameters.get().getString("VC_ALARM_NAME_BASE");

   private Client _controlClient; // used for VC control operations and is the parent client for the others
   private Client _waitForUpdateClient;   // used for the main waitForPropertyChange loop
   private Client _statsPollClient;   // used for VC stats collection
   private VcVlsi _vcVlsi;
   private final VcCredentials _vcCreds;
   private final String _rootFolderName; // root folder for this VHM instance
   private String _waitForUpdatesVersion = "";

   private Alarm _alarm = null;

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
         _controlClient = _vcVlsi.connect(_vcCreds, useCert, null, VC_CONTROL_CONNECTION_TIMEOUT_MILLIS);
         _waitForUpdateClient = _vcVlsi.connect(_vcCreds, useCert, _controlClient, VC_WAIT_FOR_UPDATES_CONNECTION_TIMEOUT_MILLIS);
         _statsPollClient = _vcVlsi.connect(_vcCreds, useCert, _controlClient, VC_STATS_POLL_CONNECTION_TIMEOUT_MILLIS);
         if ((_controlClient == null) || (_waitForUpdateClient == null) || (_statsPollClient == null)) {
            _log.log(Level.WARNING, "Unable to get VC client");
            return false;
         }
         return true;
      } catch (Exception e) {
         _log.warning("VHM: connection to vCenter failed ("+e.getClass()+"): "+e.getMessage());
         return false;
      }
   }

   private boolean connect() {
      boolean useCert = false;
      if ((_vcCreds.keyStoreFile != null) && (_vcCreds.keyStorePwd != null) && (_vcCreds.vcExtKey != null)) {
         useCert = true;
      }
      boolean success = initClients(useCert);
      if (useCert && !success && (_vcCreds.user != null) && (_vcCreds.password != null)) {
         _log.warning("VHM: certificate based login failed, trying with username and password");
         success = initClients(false);
      }
      if (!success) {
         _log.warning("VHM: could not obtain vCenter connection through any protocol");
         return false;
      }
      return true;
   }

   /*
    *  Reconnect to VC if connection timed out on either session
    *  Checking both session will also have the side effect of keep both sessions active whenever validate is called.
    */
   private boolean validateConnection(Client client) {
      boolean success = _vcVlsi.testConnection(client);
      if (!success) {
         _log.warning("VHM: connection to vCenter dropped, attempting reconnection");
         return connect();
      }
      return success;
   }

   private Alarm getAlarm() {
      if (_alarm != null) {
         return _alarm;
      }

      AlarmManager manager = getAlarmManager();
      if (manager == null) {
         return null;
      }

      Folder root;
      try {
         root = _vcVlsi.getFolderForName(_controlClient, null, _rootFolderName);

         ManagedObjectReference[] existing = manager.getAlarm(root._getRef());
         for (ManagedObjectReference m : existing) {
            Alarm a = _controlClient.createStub(Alarm.class, m);
            if (a.getInfo().getName().startsWith(VC_ALARM_NAME_BASE)) {
               _alarm = a;
               return _alarm;
            }
         }
      } catch (InvalidProperty e) {
         _log.info("VHM: unable to get reference to alarm "+VC_ALARM_NAME_BASE+" on vApp folder "+_rootFolderName);
         _log.log(Level.FINER, "VHM: exception while getting reference to top level alarm", e);
      } catch (NullPointerException e) {
         /* almost any of the values returned from vlsi client or their subsequent calls could be null but
          * will not be most of the time. It's much clearer to just have one catch here than tests on
          * ever access given we do the same thing in response.
          */
         _log.info("VHM: unable to get reference to alarm "+VC_ALARM_NAME_BASE+" on vApp folder "+_rootFolderName);
         _log.log(Level.FINER, "VHM: exception while getting reference to top level alarm", e);
      }

      return null;
   }

   public  <T extends ManagedObject> T createStub(Class<T> klass, ManagedObjectReference ref) {
      return _controlClient.createStub(klass, ref);
   }

   public VcAdapter(VcCredentials vcCreds, String rootFolderName) {
      _rootFolderName = rootFolderName;
      _vcCreds = vcCreds;
      _vcVlsi = new VcVlsi();
      if (!connect()) {
         _log.warning("VHM: could not initialize connection to vCenter");
      }
   }

   private boolean resetConnection() {
      _controlClient = _waitForUpdateClient = _statsPollClient = null;
      _vcVlsi.resetConnection();
      _vcVlsi.setThreadLocalCompoundStatus(_threadLocalStatus);
      _waitForUpdatesVersion = "";
      return connect();
   }

   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean powerOn) {
      if (!validateConnection(_controlClient)) {
         return null;
      }
      Map<String, Task> taskList = null;
      if (powerOn) {
         taskList = _vcVlsi.powerOnVMs(_controlClient, vmMoRefs);
      } else {
         taskList = _vcVlsi.powerOffVMs(_controlClient, vmMoRefs);
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
               return _vcVlsi.waitForTask(_controlClient, task);
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
   public List<VMEventData> waitForPropertyChange(String folderName) throws InterruptedException {
      if (!validateConnection(_waitForUpdateClient)) {
         return null;
      }
      List<VMEventData> result = new ArrayList<VMEventData>();
      for (int i=0; i<2; i++) {
         String versionStatus = _vcVlsi.waitForUpdates(_waitForUpdateClient, folderName, _waitForUpdatesVersion, result);
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_CANCELED_STATUS)) {
            throw new InterruptedException();
         } else
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_INVALID_COLLECTOR_VERSION_STATUS)) {
            resetConnection();
            _waitForUpdatesVersion = "";
            continue;
         } else
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_INVALID_PROPERTY_STATUS)) {
            /* We should never see this */
            resetConnection();
            continue;
         } else
         if (versionStatus.equals(VcVlsi.WAIT_FOR_UPDATES_NO_CLUSTERS)) {
            /* If no clusters are yet created, VHM should be pretty much dormant */
            Thread.sleep(SLEEP_RETRY_LOOKING_FOR_VALID_CLUSTERS);
         } else if (!_waitForUpdatesVersion.equals(versionStatus)) {
            _log.fine("Updating waitForUpdates version to "+versionStatus);
            _waitForUpdatesVersion = versionStatus;
         }
         break;
      }
      return result;
   }

   @Override
   public PerformanceManager getPerformanceManager() {
      if (!validateConnection(_statsPollClient)) {
         return null;
      }
      return _vcVlsi.getPerformanceManager(_statsPollClient);
   }

   @Override
   public EventManager getEventManager() {
      if (!validateConnection(_controlClient)) {
         return null;
      }
      return _vcVlsi.getEventManager(_controlClient);
   }

   @Override
   public AlarmManager getAlarmManager() {
      if (!validateConnection(_controlClient)) {
         return null;
      }
      return _vcVlsi.getAlarmManager(_controlClient);
   }

   @Override
   public ExtensionManager getExtensionManager() {
      if (!validateConnection(_controlClient)) {
         return null;
      }
      return _vcVlsi.getExtensionManager(_controlClient);
   }

   @Override
   public List<String> listVMsInFolder(String folderName) {
      if (!validateConnection(_controlClient)) {
         return null;
      }
      return _vcVlsi.getVMsInFolder(_controlClient, _rootFolderName, folderName);
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
    */
   @Override
   public void log(EventSeverity level, String vmMoRef, String message) {
      EventManager eventManager = getEventManager();

      ManagedObjectReference ref = new ManagedObjectReference();
      ref.setValue(vmMoRef);
      ref.setType("VirtualMachine");

      EventEx event = new EventExImpl();
      event.setCreatedTime(Calendar.getInstance());
      event.setUserName("Big Data Extensions");
      event.setEventTypeId("com.vmware.vhadoop.vhm.vc.events."+level.name());
      event.setSeverity(level.name());
      event.setMessage(message);
      event.setObjectId(ref.getValue());
      event.setObjectType(new TypeNameImpl("VirtualMachine"));

      try {
         _log.log(VhmLevel.USER, "VHM: <%V"+vmMoRef+"%V> - "+message);
         eventManager.postEvent(event, null);
      } catch (InvalidEvent e) {
         _log.log(Level.INFO, "VHM: <%V"+vmMoRef+"%V> - failed to log "+level.name()+" event with vCenter", e);
      }
   }


   @Override
   public void raiseAlarm(String vmMoRef, String message) {
      log(EventSeverity.warning, vmMoRef, message);
   }

   @Override
   public void clearAlarm(String vmMoRef) {
      /* switch the VM back to green */
      log(EventSeverity.info, vmMoRef, "all health issues previously reported by Big Data Extensions are in remission");

      AlarmManager alarmMgr = getAlarmManager();
      if (alarmMgr == null) {
         return;
      }

      /* acknowledge the alarm */
      Alarm alarm = getAlarm();
      if (alarm != null) {
         ManagedObjectReference moRef = new ManagedObjectReference();
         moRef.setValue(vmMoRef);
         moRef.setType("VirtualMachine");

         alarmMgr.acknowledgeAlarm(alarm._getRef(), moRef);
      }
   }
}
