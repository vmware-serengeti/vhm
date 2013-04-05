package com.vmware.vhadoop.api.vhm;

import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;

/* Represents read-only and idempotent methods for ClusterMap */
public interface ClusterMap {
   
   public interface ExtraInfoToScaleStrategyMapper {

      String getStrategyKey(VMEventData vmd);
   }

   Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState);
   
   Set<String> listComputeVMsForPowerState(boolean powerState);

   String getClusterIdForFolder(String clusterFolderName);

   Map<String, String> getHostIdsForVMs(Set<String> vmsToED);

   ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId);

   boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState);

   String[] getAllKnownClusterIds();

}
