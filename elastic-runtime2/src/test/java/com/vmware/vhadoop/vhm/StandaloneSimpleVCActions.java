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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vim.binding.vim.ExtensionManager;
import com.vmware.vim.binding.vim.PerformanceManager;
import com.vmware.vim.binding.vim.alarm.AlarmManager;
import com.vmware.vim.binding.vim.event.Event.EventSeverity;
import com.vmware.vim.binding.vim.event.EventManager;

public class StandaloneSimpleVCActions implements VCActions {
   Map<String, Object[]> _latestArgs = new HashMap<String, Object[]>();
   final PropertyChangeValues _propertyChangeValues = new PropertyChangeValues();
   boolean _isReady = false;
   Map<String, List<String>> _vmsInFolderMap = new HashMap<String, List<String>>();

   class PropertyChangeValues {
      VMEventData _eventToReturn;
   }

   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs,
         boolean b) {
      _latestArgs.put("changeVMPowerState", new Object[]{vmMoRefs, b});
      return null;
   }

   @Override
   public List<VMEventData> waitForPropertyChange(String folderName) throws InterruptedException {
      _latestArgs.put("waitForPropertyChange", new Object[]{folderName});
      synchronized(_propertyChangeValues) {
         _isReady = true;
         _propertyChangeValues.notify();
         _propertyChangeValues.wait();
      }
      List<VMEventData> returnVal = new ArrayList<VMEventData>();
      returnVal.add(_propertyChangeValues._eventToReturn);
      return returnVal;
   }

   Object[] getLatestMethodArgs(String methodName) {
      return _latestArgs.get(methodName);
   }

   @Override
   public void interruptWait() {
      synchronized(_propertyChangeValues) {
         _propertyChangeValues.notifyAll();
      }
      _latestArgs.put("interruptWait", new Object[]{});
   }

   @Override
   public List<String> listVMsInFolder(String folderName) {
      _latestArgs.put("listVMsInFolder", new Object[]{folderName});
      return _vmsInFolderMap.get(folderName);
   }

   protected void addVMToFolder(String folderName, String vmId) {
      List<String> vms = _vmsInFolderMap.get(folderName);
      if (vms == null) {
         vms = new ArrayList<String>();
         _vmsInFolderMap.put(folderName, vms);
      }
      vms.add(vmId);
   }

   public void fakeWaitForUpdatesData(VMEventData eventToReturn) {
      /* It's essential that we don't fake any data until a thread is actually waiting for the update */
      while (!_isReady) {
         try {
            Thread.sleep(10);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
      synchronized(_propertyChangeValues) {
         _propertyChangeValues._eventToReturn = eventToReturn;
         _propertyChangeValues.notify();
         try {
            _propertyChangeValues.wait();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
   }

   @Override
   public PerformanceManager getPerformanceManager() {
      _latestArgs.put("getPerformanceManager", new Object[]{});
      return null;
   }

   @Override
   public EventManager getEventManager() {
      _latestArgs.put("getEventManager", new Object[]{});
      return null;
   }

   @Override
   public AlarmManager getAlarmManager() {
      _latestArgs.put("getAlarmManager", new Object[]{});
      return null;
   }

   @Override
   public ExtensionManager getExtensionManager() {
      _latestArgs.put("getExtensionManager", new Object[]{});
      return null;
   }

   @Override
   public void log(EventSeverity level, String vmMoRef, String message) {
      _latestArgs.put("log", new Object[]{});
      return;
   }
}
