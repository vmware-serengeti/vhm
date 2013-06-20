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

import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;

/* Represents read-only and idempotent methods for ClusterMap
 * Everything returned by this interface should be a deep copy of the ClusterMap data
 * TODO: Check that this is correct */
public interface ClusterMap {

   public interface ExtraInfoToClusterMapper {

      /* Returns the key which indicates the scale strategy singleton to use for this cluster */
      String getStrategyKey(SerengetiClusterVariableData clusterData, String clusterId);
      
      /* Allows for the addition of contextual data to be added to a cluster and retrieved through ClusterMap */
      Map<String, String> parseExtraInfo(SerengetiClusterVariableData clusterData, String clusterId);

      /* Allows for the creation of new scale events based on cluster state change */
      Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData clusterData, String clusterId, boolean isNewCluster);
   }

   Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState);

   Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState);

   Set<String> listComputeVMsForPowerState(boolean powerState);

   Set<String> listHostsWithComputeVMsForCluster(String clusterId);

   String getClusterIdForFolder(String clusterFolderName);

   Map<String, String> getHostIdsForVMs(Set<String> vmsToED);

   ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId);

   Boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState);

   String[] getAllKnownClusterIds();
   
   String[] getAllClusterIdsForScaleStrategyKey(String key);

   HadoopClusterInfo getHadoopInfoForCluster(String clusterId);

   Map<String, String> getDnsNameForVMs(Set<String> vmIds);

   String getHostIdForVm(String vmId);

   String getClusterIdForVm(String vmIds);

   String getScaleStrategyKey(String clusterId);
   
   Integer getNumVCPUsForVm(String vmId);
   
   Long getPowerOnTimeForVm(String vmId);
   
   String getExtraInfo(String clusterId, String key);
}
