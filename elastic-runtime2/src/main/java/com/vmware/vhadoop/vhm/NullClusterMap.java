package com.vmware.vhadoop.vhm;

import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public class NullClusterMap implements ClusterMap
{
   @Override
   public Set<String> listComputeVMsForCluster(String clusterId) {
      return null;
   }

   @Override
   public Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState) {
      return null;
   }

   @Override
   public Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState) {
      return null;
   }

   @Override
   public Set<String> listComputeVMsForPowerState(boolean powerState) {
      return null;
   }

   @Override
   public Set<String> listHostsWithComputeVMsForCluster(String clusterId) {
      return null;
   }

   @Override
   public String getClusterIdForName(String clusterFolderName) {
      return null;
   }

   @Override
   public Map<String, String> getHostIdsForVMs(Set<String> vmsToED) {
      return null;
   }

   @Override
   public ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId) {
      return null;
   }

   @Override
   public Boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState) {
      return null;
   }

   @Override
   public Boolean checkPowerStateOfVm(String vmId, boolean expectedPowerState) {
      return null;
   }

   @Override
   public Set<String> getAllKnownClusterIds() {
      return null;
   }

   @Override
   public Set<String> getAllClusterIdsForScaleStrategyKey(String key) {
      return null;
   }

   @Override
   public HadoopClusterInfo getHadoopInfoForCluster(String clusterId) {
      return null;
   }

   @Override
   public Map<String, String> getDnsNamesForVMs(Set<String> vmIds) {
      return null;
   }

   @Override
   public String getDnsNameForVM(String vmId) {
      return null;
   }

   @Override
   public Map<String, String> getVmIdsForDnsNames(Set<String> dnsNames) {
      return null;
   }

   @Override
   public String getVmIdForDnsName(String dnsName) {
      return null;
   }

   @Override
   public String getHostIdForVm(String vmId) {
      return null;
   }

   @Override
   public String getClusterIdForVm(String vmIds) {
      return null;
   }

   @Override
   public String getScaleStrategyKey(String clusterId) {
      return null;
   }

   @Override
   public Integer getNumVCPUsForVm(String vmId) {
      return null;
   }

   @Override
   public Long getPowerOnTimeForVm(String vmId) {
      return null;
   }

   @Override
   public Long getPowerOffTimeForVm(String vmId) {
      return null;
   }

   @Override
   public String getExtraInfo(String clusterId, String key) {
      return null;
   }

   @Override
   public String getMasterVmIdForCluster(String clusterId) {
      return null;
   }

   @Override
   public Map<String, Set<String>> getNicAndIpAddressesForVm(String vmId) {
      return null;
   }

}
