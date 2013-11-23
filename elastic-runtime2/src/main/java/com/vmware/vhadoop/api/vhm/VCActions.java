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

package com.vmware.vhadoop.api.vhm;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.vmware.vim.binding.vim.PerformanceManager;
import com.vmware.vim.binding.vim.event.Event.EventSeverity;

/* Represents actions which can be invoked on the VC subsystem */
public interface VCActions {

   public static final String VC_POWER_ON_STATUS_KEY = "powerOnVM";
   public static final String VC_POWER_OFF_STATUS_KEY = "powerOffVM";

   public class MasterVmEventData {
      public Boolean _enableAutomation;
      public Integer _minInstances;
      public Integer _maxInstances;
      public Integer _jobTrackerPort;

      @Override
      public String toString() {
         return "enableAutomation="+_enableAutomation+", minInstances="+_minInstances+", maxInstances="+_maxInstances+", jobTrackerPort="+_jobTrackerPort;
      }
   }

   public class VMEventData {
      // these two fields must always be filled
      public String _vmMoRef;
      public Boolean _isLeaving;

      // these fields can be left as null if there is no new information
      /* TODO: Split into fields which are constant and variable and check that constants are not being changed */
      public Boolean _isElastic;
      public String _myName;
      public String _myUUID;
      public String _hostMoRef;
      public String _serengetiFolder;
      public String _masterUUID;
      public Boolean _powerState;
      public String _masterMoRef;
      public Map<String, Set<String>> _nicAndIpAddressMap;
      public String _dnsName;
      public Integer _vCPUs;

      /* If this is non-null, we derive that this is information about a master VM */
      public MasterVmEventData _masterVmData;

      @Override
      public String toString() {
         return "<%V"+_vmMoRef+"%V>, isLeaving="+_isLeaving+", isElastic="+_isElastic+", myName="+_myName+", myUUID="+_myUUID+", hostMoRef="+_hostMoRef+", serengetiFolder="+
                     _serengetiFolder+", masterUUID="+_masterUUID+", powerState="+_powerState+", masterMoRef="+_masterMoRef+", _nicAndIpAddressMap="+_nicAndIpAddressMap+
                     ", dnsName="+_dnsName+", vCPUs="+_vCPUs+", masterVMData={"+_masterVmData+"}";
      }
   }

   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean b);

   public void interruptWait();

   public PerformanceManager getPerformanceManager();
   
   /* Should only ever be called by the ClusterStateChangeListener Thread */
   public List<VMEventData> waitForPropertyChange(String folderName) throws InterruptedException;

   public List<String> listVMsInFolder(String folderName);

   public boolean logEventForVM(EventSeverity level, String vmMoRef, String message);

   public void raiseAlarm(String vmMoRef, String detail);

   public void clearAlarm(String vmMoRef);
}
