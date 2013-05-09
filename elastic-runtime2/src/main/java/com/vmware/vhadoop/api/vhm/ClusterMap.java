package com.vmware.vhadoop.api.vhm;

import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;

/* Represents read-only and idempotent methods for ClusterMap
 * Everything returned by this interface should be a deep copy of the ClusterMap data
 * TODO: Check that this is correct */
public interface ClusterMap {

   public interface ExtraInfoToScaleStrategyMapper {

      String getStrategyKey(VMEventData vmd);
   }

   Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState);

   Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState);

   Set<String> listComputeVMsForPowerState(boolean powerState);

   Set<String> listHostsWithComputeVMsForCluster(String clusterId);

   String getClusterIdForFolder(String clusterFolderName);

   Map<String, String> getHostIdsForVMs(Set<String> vmsToED);

   ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId);

   boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState);

   String[] getAllKnownClusterIds();

   String getScaleStrategyKey(String clusterId);

   HadoopClusterInfo getHadoopInfoForCluster(String clusterId);

   Set<String> getDnsNameForVMs(Set<String> vms);

   String getHostIdForVm(String vmid);

   String getClusterIdForVm(String vm);
   
   Integer getNumVCPUsForVm(String vm);
   
   long getPowerOnTimeForVm(String vm);
}
