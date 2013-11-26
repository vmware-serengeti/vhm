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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;
import com.vmware.vhadoop.api.vhm.VCActions.MasterVmEventData;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.vc.VcVlsi;

public abstract class AbstractJUnitTest {
   
   Set<String> _clusterNames = new HashSet<String>();
   Set<String> _masterVmNames = new HashSet<String>();
   Set<String> _vmNames = new HashSet<String>();

   final String CLUSTER_NAME_PREFIX = "myTestCluster_";
   final String VM_NAME_PREFIX = "myTestVm_";
   final String DNS_PREFIX = "DNS_";
   final String NIC_PREFIX = "NIC_";
   final String IPADDR_PREFIX = "IPADDR_";
   final String UUID_PREFIX = "UUID_";
   final String MOREF_PREFIX = "MOREF_";
   final String FOLDER_PREFIX = "FOLDER_";
   final String HOST_PREFIX = "HOST_";
   final String MASTER_VM_NAME_POSTFIX = VcVlsi.SERENGETI_MASTERVM_NAME_POSTFIX;
   
   final String OTHER_SCALE_STRATEGY_KEY = "otherScaleStrategy";
   final String DEFAULT_SCALE_STRATEGY_KEY = "defaultScaleStrategy";
   
   final int DEFAULT_PORT = 1234;
   final int DEFAULT_VCPUS = 2;

   protected ClusterMap getTestClusterMap(boolean prepopulate) {
      return new StandaloneSimpleClusterMap(prepopulate);
   }
   
   protected ClusterMapReader getTestClusterMapReader(ClusterMap clusterMap) {
      MultipleReaderSingleWriterClusterMapAccess.destroy();
      MultipleReaderSingleWriterClusterMapAccess cma = 
            MultipleReaderSingleWriterClusterMapAccess.getClusterMapAccess(clusterMap);
      return new AbstractClusterMapReader(cma, null) {};
   }
   
   VMEventData createEventData(String clusterName, String vmName, boolean isMaster, 
         Boolean powerState, String hostName, String masterVmName,
         boolean autoCluster, Integer minClusterInstances, Integer maxClusterInstances, boolean isNewVM) {
      VMEventData result = new VMEventData();
      if (isNewVM) {
         result._myUUID = UUID_PREFIX+vmName;
      }
      result._dnsName = getDnsNameFromVmName(vmName);
      result._hostMoRef = MOREF_PREFIX+hostName;
      result._nicAndIpAddressMap = new HashMap<String, Set<String>>();
      result._nicAndIpAddressMap.put(NIC_PREFIX+vmName, 
            new HashSet<String>(Arrays.asList(new String[]{IPADDR_PREFIX+vmName+"1", IPADDR_PREFIX+vmName+"2"})));
      result._masterMoRef = MOREF_PREFIX+masterVmName;
      result._masterUUID = UUID_PREFIX+vmName;
      result._masterUUID = getClusterIdForMasterVmName(masterVmName);
      result._myName = vmName;
      result._serengetiFolder = getFolderNameForClusterName(clusterName);
      result._vmMoRef = MOREF_PREFIX+vmName;
      result._vCPUs = DEFAULT_VCPUS;
      result._isElastic = !isMaster;
      if (isMaster) {
         result._masterVmData = new MasterVmEventData();
         result._masterVmData._enableAutomation = autoCluster;
         result._masterVmData._minInstances = minClusterInstances;
         result._masterVmData._maxInstances = maxClusterInstances;
         result._masterVmData._jobTrackerPort = DEFAULT_PORT;
         result._masterVmData._clusterName = clusterName;
      }
      result._powerState = powerState;
      result._isLeaving = false;
      return result;
   }
   
   void populateClusterSameHost(String clusterName, String hostName, int numVms, boolean defaultPowerState, 
         boolean autoCluster, Integer minInstances, Set<ClusterScaleEvent> impliedScaleEvents) {
      String masterVmName = null;
      for (int i=0; i<numVms; i++) {
         String vmName;
         if (i==0) {
            vmName = masterVmName = clusterName+MASTER_VM_NAME_POSTFIX+"_"+VM_NAME_PREFIX+i;
            _masterVmNames.add(masterVmName);
         } else {
            vmName = clusterName+"_"+VM_NAME_PREFIX+i;
         }
         _vmNames.add(vmName);
         VMEventData eventData = createEventData(clusterName, vmName, i==0, (i==0 || defaultPowerState), hostName, masterVmName, autoCluster, minInstances, -1, true);
         processNewEventData(eventData, deriveClusterIdFromClusterName(clusterName), impliedScaleEvents);
      }
      registerScaleStrategy(new TrivialScaleStrategy(DEFAULT_SCALE_STRATEGY_KEY));
      registerScaleStrategy(new TrivialScaleStrategy(OTHER_SCALE_STRATEGY_KEY));
   }
   
   /* Override */
   void processNewEventData(VMEventData eventData, String expectedClusterId, Set<ClusterScaleEvent> impliedScaleEvents) {}
   
   /* Override */
   void registerScaleStrategy(ScaleStrategy scaleStrategy) {}

   void populateSimpleClusterMap(int numClusters, int vmsPerCluster, boolean defaultPowerState, Set<ClusterScaleEvent> impliedScaleEvents) {
      for (int i=0; i<numClusters; i++) {
         String clusterName = CLUSTER_NAME_PREFIX+i;
         _clusterNames.add(clusterName);
         Integer minInstances = i;
         /* Default automation value is true, so that if it's false, we can assert that VHM defaults to the real ManualScaleStrategy */
         populateClusterSameHost(clusterName, "DEFAULT_HOST", vmsPerCluster, defaultPowerState, true, minInstances, impliedScaleEvents);
      }
   }

   void populateSimpleClusterMap(int numClusters, int vmsPerCluster, boolean defaultPowerState) {
      populateSimpleClusterMap(numClusters, vmsPerCluster, defaultPowerState, null);
   }

   void populateClusterPerHost(int numClusters, int vmsPerCluster, boolean defaultPowerState, Set<ClusterScaleEvent> impliedScaleEvents) {
      for (int i=0; i<numClusters; i++) {
         String clusterName = CLUSTER_NAME_PREFIX+i;
         _clusterNames.add(clusterName);
         Integer minInstances = i;
         /* Default automation value is true, so that if it's false, we can assert that VHM defaults to the real ManualScaleStrategy */
         populateClusterSameHost(clusterName, HOST_PREFIX+i, vmsPerCluster, defaultPowerState, true, minInstances, impliedScaleEvents);
      }
   }

   void populateClusterPerHost(int numClusters, int vmsPerCluster, boolean defaultPowerState) {
      populateClusterPerHost(numClusters, vmsPerCluster, defaultPowerState, null);
   }

   String getClusterIdForMasterVmName(String masterVmName) {
      return UUID_PREFIX+masterVmName;
   }

   String getFolderNameForClusterName(String clusterName) {
      return FOLDER_PREFIX+clusterName;
   }
   
   String getDnsNameFromVmName(String vmName) {
      return DNS_PREFIX+vmName;
   }
   
   String getMasterVmIdForCluster(String clusterName) {
      return getVmIdFromVmName(getMasterVmNameForCluster(clusterName));
   }

   String getMasterVmNameForCluster(String clusterName) {
      return clusterName+MASTER_VM_NAME_POSTFIX+"_"+VM_NAME_PREFIX+0;
   }

   String deriveClusterIdFromClusterName(String clusterName) {
      return getClusterIdForMasterVmName(getMasterVmNameForCluster(clusterName));
   }
   
   String deriveClusterIdForComputeVmName(String vmName) {
      String clusterName = vmName.substring(0, vmName.indexOf('_', vmName.indexOf('_')+1));
      return deriveClusterIdFromClusterName(clusterName);
   }
   
   Object deriveMasterDnsNameFromClusterId(String clusterId) {
      String removedUUID = clusterId.substring(clusterId.indexOf('_')+1);
      return DNS_PREFIX+removedUUID;
   }

   String getVmIdFromVmName(String vmName) {
      return MOREF_PREFIX+vmName;
   }

   Set<String> getVmIdsFromVmNames(Set<String> vmNames) {
      Set<String> result = new HashSet<String>();
      for (String vmName : vmNames) {
         result.add(getVmIdFromVmName(vmName));
      }
      return result;
   }

   String deriveClusterIdFromVmName(String vmName) {
      String temp;
      if (vmName.contains(MASTER_VM_NAME_POSTFIX)) {
         temp = getClusterIdForMasterVmName(vmName);
      } else {
         temp = deriveClusterIdForComputeVmName(vmName);
      }
      String stripTrailingNumber = temp.substring(0, temp.lastIndexOf('_')+1);
      return stripTrailingNumber+"0";
   }
   
   String deriveHostIdFromVmName(String vmName) {
      int firstUnderscore = vmName.indexOf('_');
      String clusterNumber;
      if (vmName.contains(MASTER_VM_NAME_POSTFIX)) {
         clusterNumber = vmName.substring(firstUnderscore+1, vmName.lastIndexOf(MASTER_VM_NAME_POSTFIX));
      } else {
         clusterNumber = vmName.substring(firstUnderscore+1, vmName.indexOf('_', firstUnderscore+1));
      }
      return MOREF_PREFIX+HOST_PREFIX+clusterNumber;
   }

   String deriveHostIdFromVmId(String vmId) {
      return deriveHostIdFromVmName(vmId.substring(MOREF_PREFIX.length()));
   }
   
   Set<String> getBogusSet() {
      Set<String> result = new HashSet<String>();
      result.add("bogus");
      return result;
   }
   
   Set<String> getEmptySet() {
      return new HashSet<String>();
   }
}
