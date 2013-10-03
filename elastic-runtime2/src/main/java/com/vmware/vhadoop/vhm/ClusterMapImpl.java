package com.vmware.vhadoop.vhm;

import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;

/* This is a non-caching implementation of ClusterMap which simply delegates directly to the base methods in BaseClusterMap
 * 
 * The reason for this delegation is that CachingClusterMapImpl invokes these Base methods using reflection, by taking the method
 * name of the currently executing method and post-fixing "Base" to them. 
 */
public class ClusterMapImpl extends BaseClusterMap {

   ClusterMapImpl(ExtraInfoToClusterMapper mapper) {
      super(mapper);
   }

   @Override
   public Set<String> listComputeVMsForCluster(String clusterId) {
      return listComputeVMsForClusterBase(clusterId);
   }
   
   @Override
   public Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState) {
      return listComputeVMsForClusterAndPowerStateBase(clusterId, powerState);
   }
   
   @Override
   public Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState) {
      return listComputeVMsForClusterHostAndPowerStateBase(clusterId, hostId, powerState);
   }

   @Override
   public Set<String> listComputeVMsForPowerState(boolean powerState) {
      return listComputeVMsForPowerStateBase(powerState);
   }

   @Override
   public Set<String> listHostsWithComputeVMsForCluster(String clusterId) {
      return listHostsWithComputeVMsForClusterBase(clusterId);
   }

   @Override
   public Map<String, String> getHostIdsForVMs(Set<String> vmsToED) {
      return getHostIdsForVMsBase(vmsToED);
   }

   @Override
   public Boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState) {
      return checkPowerStateOfVmsBase(vmIds, expectedPowerState);
   }

   @Override
   public Boolean checkPowerStateOfVm(String vmId, boolean expectedPowerState) {
      return checkPowerStateOfVmBase(vmId, expectedPowerState);
   }

   @Override
   public Map<String, String> getDnsNamesForVMs(Set<String> vmIds) {
      return getDnsNamesForVMsBase(vmIds);
   }

   @Override
   public String getDnsNameForVM(String vmId) {
      return getDnsNameForVMBase(vmId);
   }

   @Override
   public Map<String, String> getVmIdsForDnsNames(Set<String> dnsNames) {
      return getVmIdsForDnsNamesBase(dnsNames);
   }

   @Override
   public String getVmIdForDnsName(String dnsName) {
      return getVmIdForDnsNameBase(dnsName);
   }

   @Override
   public String getClusterIdForFolder(String clusterFolderName) {
      return getClusterIdForFolderBase(clusterFolderName);
   }

   @Override
   public String[] getAllClusterIdsForScaleStrategyKey(String key) {
      return getAllClusterIdsForScaleStrategyKeyBase(key);
   }

   @Override
   public String getScaleStrategyKey(String clusterId) {
      return getScaleStrategyKeyBase(clusterId);
   }

   @Override
   public HadoopClusterInfo getHadoopInfoForCluster(String clusterId) {
      return getHadoopInfoForClusterBase(clusterId);
   }

}
