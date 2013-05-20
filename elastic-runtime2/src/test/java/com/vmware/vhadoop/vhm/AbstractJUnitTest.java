package com.vmware.vhadoop.vhm;

import java.util.HashSet;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.MasterVmEventData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;

public abstract class AbstractJUnitTest {
   
   Set<String> _clusterNames = new HashSet<String>();
   Set<String> _masterVmNames = new HashSet<String>();
   Set<String> _vmNames = new HashSet<String>();

   final String CLUSTER_NAME_PREFIX = "myTestCluster_";
   final String VM_NAME_PREFIX = "myTestVm_";
   final String DNS_PREFIX = "DNS_";
   final String IPADDR_PREFIX = "IPADDR_";
   final String UUID_PREFIX = "UUID_";
   final String MOREF_PREFIX = "MOREF_";
   final String FOLDER_PREFIX = "FOLDER_";
   final String HOST_PREFIX = "HOST_";
   
   final String AUTO_SCALE_STRATEGY_KEY = "autoScaleStrategy";
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
         boolean powerState, String hostName, String masterVmName,
         boolean autoCluster, Integer minClusterInstances) {
      VMEventData result = new VMEventData();
      result._dnsName = getDnsNameFromVmName(vmName);
      result._hostMoRef = MOREF_PREFIX+hostName;
      result._ipAddr = IPADDR_PREFIX+vmName;
      result._masterMoRef = MOREF_PREFIX+masterVmName;
      result._masterUUID = UUID_PREFIX+vmName;
      result._masterUUID = getClusterIdForMasterVmName(masterVmName);
      result._myName = vmName;
      result._serengetiFolder = getFolderNameForClusterName(clusterName);
      result._vmMoRef = MOREF_PREFIX+vmName;
      result._vCPUs = DEFAULT_VCPUS;
      if (isMaster) {
         result._masterVmData = new MasterVmEventData();
         result._masterVmData._enableAutomation = autoCluster;
         result._masterVmData._minInstances = minClusterInstances;
         result._masterVmData._jobTrackerPort = DEFAULT_PORT;
      } else {
         result._isElastic = true;
      }
      result._powerState = powerState;
      result._isLeaving = false;
      return result;
   }
   
   void populateClusterSameHost(String clusterName, String hostName, int numVms, boolean defaultPowerState, 
         boolean autoCluster, Integer minInstances) {
      String masterVmName = null;
      for (int i=0; i<numVms; i++) {
         String vmName = clusterName+"_"+VM_NAME_PREFIX+i;
         _vmNames.add(vmName);
         if (i==0) {
            masterVmName = vmName;
            _masterVmNames.add(masterVmName);
         }
         VMEventData eventData = createEventData(clusterName, vmName, i==0, defaultPowerState, hostName, masterVmName, autoCluster, minInstances);
         processNewEventData(eventData);
      }
      registerScaleStrategy(new TrivialScaleStrategy(DEFAULT_SCALE_STRATEGY_KEY));
      registerScaleStrategy(new TrivialScaleStrategy(AUTO_SCALE_STRATEGY_KEY));
   }
   
   /* Override */
   void processNewEventData(VMEventData eventData) {}
   
   /* Override */
   void registerScaleStrategy(ScaleStrategy scaleStrategy) {}
   
   void populateSimpleClusterMap(int numClusters, int vmsPerCluster, boolean defaultPowerState) {
      for (int i=0; i<numClusters; i++) {
         String clusterName = CLUSTER_NAME_PREFIX+i;
         _clusterNames.add(clusterName);
         Integer minInstances = (i==0) ? null : i;
         populateClusterSameHost(clusterName, "DEFAULT_HOST", vmsPerCluster, defaultPowerState, false, minInstances);
      }
   }

   void populateClusterPerHost(int numClusters, int vmsPerCluster, boolean defaultPowerState) {
      for (int i=0; i<numClusters; i++) {
         String clusterName = CLUSTER_NAME_PREFIX+i;
         _clusterNames.add(clusterName);
         Integer minInstances = (i==0) ? (null) : i;
         populateClusterSameHost(clusterName, HOST_PREFIX+i, vmsPerCluster, defaultPowerState, false, minInstances);
      }
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
      return getVmIdFromVmName(clusterName+"_"+VM_NAME_PREFIX+0);
   }

   String deriveClusterIdFromClusterName(String clusterName) {
      return getClusterIdForMasterVmName(clusterName+"_"+VM_NAME_PREFIX+0);
   }
   
   Object deriveMasterIpAddrFromClusterId(String clusterId) {
      String removedUUID = clusterId.substring(clusterId.indexOf('_')+1);
      return IPADDR_PREFIX+removedUUID;
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
      String temp = getClusterIdForMasterVmName(vmName);
      String stripTrailingNumber = temp.substring(0, temp.lastIndexOf('_')+1);
      return stripTrailingNumber+"0";
   }
   
   String deriveHostIdFromVmName(String vmName) {
      int firstUnderscore = vmName.indexOf('_');
      String clusterNumber = vmName.substring(firstUnderscore+1, vmName.indexOf('_', firstUnderscore+1));
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
