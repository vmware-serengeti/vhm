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

import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VmType;

/* Represents read-only and idempotent methods for ClusterMap
 * Everything returned by this interface should be a deep and immutable copy of the ClusterMap data */
public interface ClusterMap {

   public interface ExtraInfoToClusterMapper {

      /* Returns the key which indicates the scale strategy singleton to use for this cluster */
      String getStrategyKey(SerengetiClusterVariableData clusterData, String clusterId);

      /* Allows for the addition of contextual data to be added to a cluster and retrieved through ClusterMap */
      Map<String, String> parseExtraInfo(SerengetiClusterVariableData clusterData, String clusterId);

      /* Allows for the creation of new scale events based on cluster state change - order is preserved of the returned set */
      /* Viability of cluster is whether jobs could currently be successfully run on it. Eg is the JobTracker powered on? */
      Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData clusterData, String clusterId, boolean isNewCluster, boolean isClusterViable);
   }

   Set<String> listComputeVMsForCluster(String clusterId);

   Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState);

   Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState);

   Set<String> listComputeVMsForPowerState(boolean powerState);

   Set<String> listHostsWithComputeVMsForCluster(String clusterId);

   String getClusterIdForName(String clusterFolderName);

   Map<String, String> getHostIdsForVMs(Set<String> vmsToED);

   ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId);

   Boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState);
   
   Boolean checkPowerStateOfVm(String vmId, boolean expectedPowerState);

   Set<String> getAllKnownClusterIds();

   Set<String> getAllClusterIdsForScaleStrategyKey(String key);

   HadoopClusterInfo getHadoopInfoForCluster(String clusterId);

   Map<String, String> getDnsNamesForVMs(Set<String> vmIds);

   String getDnsNameForVM(String vmId);

   Map<String, String> getVmIdsForDnsNames(Set<String> dnsNames);

   String getVmIdForDnsName(String dnsName);

   String getHostIdForVm(String vmId);

   String getClusterIdForVm(String vmIds);

   String getScaleStrategyKey(String clusterId);

   Integer getNumVCPUsForVm(String vmId);

   Long getPowerOnTimeForVm(String vmId);

   Long getPowerOffTimeForVm(String vmId);

   String getExtraInfo(String clusterId, String key);

   String getMasterVmIdForCluster(String clusterId);

   /* Returns NIC name as key and IpAddresses for NIC as value */
   Map<String, Set<String>> getNicAndIpAddressesForVm(String vmId);
   
   interface VMUpdateListener {
      void updatingVM(String moRef);
   }
   
   interface VMCollectionUpdateListener {
      void updatingVMCollection();
   }

   /* This data type is here to prevent direct access to internal members in any of the implementation classes */
   class VMInfo {
      private final String _moRef;
      private final VMConstantData _constantData;
      private final VMVariableData _variableData;
      private final String _clusterId;
      private long _powerOnTime; // most recent timestamp when VHM learned VM is on
      private long _powerOffTime; // most recent timestamp when VHM learned VM is off
      private VMUpdateListener _updateListener;

      /* VMInfo is created with a completed VMConstantData and a potentially incomplete or even null variableData
       * moRef and clusterId must not be null */
      public VMInfo(String moRef, VMConstantData constantData,
            VMVariableData variableData, String clusterId) {
         _moRef = moRef;
         _constantData = constantData;
         /* Provide an empty variableData if a null one is passed in */
         _variableData = (variableData != null) ? variableData : new VMVariableData();
         _clusterId = clusterId;
         if (_variableData._powerState != null) {
            if (_variableData._powerState) {
               _powerOnTime = System.currentTimeMillis();      /* Initialize to a reasonable value, even if this isn't the actual power-on time */
            } else {
               _variableData._dnsName = null;         /* VC may give us stale values for a powered-off VM on init */
            }
         }
      }
      
      public void setUpdateListener(VMUpdateListener listener) {
         _updateListener = listener;
      }
      
      private VMVariableData assignVariableData() {
         if (_updateListener != null) {
            _updateListener.updatingVM(_moRef);
         }
         return _variableData;
      }

      public String getClusterId() {
         return _clusterId;
      }

      public VmType getVmType() {
         return _constantData._vmType;
      }

      public String getHostMoRef() {
         return _variableData._hostMoRef;
      }

      public String getMyName() {
         return _variableData._myName;
      }

      public Boolean getPowerState() {
         return _variableData._powerState;
      }

      public String getDnsName() {
         return _variableData._dnsName;
      }

      public Integer getvCPUs() {
         return _variableData._vCPUs;
      }
      
      public String getMoRef() {
         return _moRef;
      }
      
      public long getPowerOnTime() {
         return _powerOnTime;
      }

      public long getPowerOffTime() {
         return _powerOffTime;
      }

      public String getMyUUID() {
         return _constantData._myUUID;
      }

      public Map<String, Set<String>> getNicAndIpAddressMap() {
         return _variableData._nicAndIpAddressMap;
      }

      public void setMyName(String myName) {
         assignVariableData()._myName = myName;
      }

      public void setHostMoRef(String hostMoRef) {
         assignVariableData()._hostMoRef = hostMoRef;
      }

      public void setPowerState(Boolean powerState) {
         assignVariableData()._powerState = powerState;
      }

      public void setDnsName(String dnsName) {
         assignVariableData()._dnsName = dnsName;
      }

      public void setNicAndIpAddressMap(Map<String, Set<String>> nicAndIpAddressMap) {
         assignVariableData()._nicAndIpAddressMap = nicAndIpAddressMap;
      }

      public void setvCPUs(Integer vCPUs) {
         assignVariableData()._vCPUs = vCPUs;
      }

      public void setPowerOnTime(long currentTimeMillis) {
         assignVariableData();
         _powerOnTime = currentTimeMillis;
      }

      public void setPowerOffTime(long currentTimeMillis) {
         assignVariableData();
         _powerOffTime = currentTimeMillis;
      }
   }

   interface ClusterUpdateListener {
      void updatingCluster(String clusterId);
   }

   interface ClusterCollectionUpdateListener {
      void updatingClusterCollection();
   }
   
   class ClusterInfo {
      private final String _masterUUID;
      private final SerengetiClusterConstantData _constantData;
      
      private Integer _jobTrackerPort;
      private String _scaleStrategyKey;
      private Map<String, String> _extraInfo;

      /* These fields don't represent cluster state */
      private LinkedList<ClusterScaleCompletionEvent> _completionEvents;
      private Long _incompleteSince;
      private ClusterUpdateListener _updateListener;

      public ClusterInfo(String clusterId, SerengetiClusterConstantData constantData) {
         this._masterUUID = clusterId;
         this._constantData = constantData;
         _completionEvents = new LinkedList<ClusterScaleCompletionEvent>();
      }

      public void setUpdateListener(ClusterUpdateListener listener) {
         _updateListener = listener;
      }
      
      private void notifyUpdate() {
         if (_updateListener != null) {
            _updateListener.updatingCluster(_masterUUID);
         }
      }

      public String getScaleStrategyKey() {
         return _scaleStrategyKey;
      }

      public Integer getJobTrackerPort() {
         return _jobTrackerPort;
      }

      public String getExtraInfoValue(String key) {
         if (_extraInfo != null) {
            return _extraInfo.get(key);
         }
         return null;
      }

      public String getMasterMoRef() {
         return _constantData._masterMoRef;
      }

      public String getClusterId() {
         return _masterUUID;
      }

      public String getExtraInfoMapAsString() {
         if (_extraInfo != null) {
            return _extraInfo.toString();
         }
         return "null";
      }

      public String getSerengetiFolder() {
         return _constantData._serengetiFolder;
      }

      public String getClusterName() {
         return _constantData._clusterName;
      }

      public ClusterScaleCompletionEvent getMostRecentCompletionEvent() {
         if (_completionEvents.size() > 0) {
            return _completionEvents.getFirst();
         }
         return null;
      }
      
      public void setScaleStrategyKey(String scaleStrategyKey) {
         notifyUpdate();
         _scaleStrategyKey = scaleStrategyKey;
      }

      public void setJobTrackerPort(Integer jobTrackerPort) {
         notifyUpdate();
         _jobTrackerPort = jobTrackerPort;
      }

      public boolean putAllExtraInfo(Map<String, String> newData) {
         boolean variableDataChanged = false;
         if (newData != null) {
            if (_extraInfo == null) {
               _extraInfo = newData;
               variableDataChanged = true;
            } else {
               for (Entry<String, String> entry : newData.entrySet()) {
                  String newValue = entry.getValue();
                  String origValue = _extraInfo.get(entry.getKey());
                  if ((newValue != null) && ((origValue == null) || !origValue.equals(newValue))) {
                     _extraInfo.put(entry.getKey(), newValue);
                     variableDataChanged = true;
                  }
               }
            }
         }
         if (variableDataChanged) {
            notifyUpdate();
         }
         return variableDataChanged;
      }

      public void addCompletionEvent(ClusterScaleCompletionEvent event) {
         /* This isn't a cluster state change */
         _completionEvents.addFirst(event);
      }

      public boolean testAndSetIsComplete(long currentTime, long graceTimeMillis) {
         /* This isn't a cluster state change */
         if (_incompleteSince == null) {
            _incompleteSince = currentTime;
            return false;
         } else if ((_incompleteSince > (currentTime - graceTimeMillis) || (graceTimeMillis == 0))) {
            return false;
         }
         return true;
      }

      public void resetIsCompleteTime() {
         /* This isn't a cluster state change */
         _incompleteSince = null;
      }
   }
}
