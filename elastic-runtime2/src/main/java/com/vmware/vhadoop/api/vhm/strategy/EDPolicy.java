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

package com.vmware.vhadoop.api.vhm.strategy;

import java.util.*;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

/**
 * Component that manages the end-to-end enabling or disabling of one or more Task Tracker VMs
 * All methods are synchronous and block until the enabling or disabling is completed
 * The set returned by the enable/disable methods be a subset of the set passed in and this doesn't necessarily indicate an error
 * All methods may return null in the case of an error, or if there is no action to take
 */
public interface EDPolicy extends ClusterMapReader {

   /**
    * Enable a candidate set of VMs for a target cluster
    * 
    * @param toEnable A set of VM ids that must belong to the same cluster
    * @param totalTargetEnabled The total target number of Task Tracker VMs that should be enabled for the cluster on completion
    * @param clusterId The cluster that the VM ids belong to
    * @return The Set of VM ids that was actually enabled, or null
    * @throws Exception
    */
   Set<String> enableTTs(Set<String> toEnable, int totalTargetEnabled, String clusterId) throws Exception;

   /**
    * Enable a candidate set of VMs for a target cluster
    * Optional context for each VM is passed through as an object value in the toEnable map
    * 
    * @param toEnable A map of VM ids that must belong to the same cluster, along with optional Object context
    * @param totalTargetEnabled The total target number of Task Tracker VMs that should be enabled for the cluster on completion
    * @param clusterId The cluster that the VM ids belong to
    * @return The Set of VM ids that was actually enabled, or null
    * @throws Exception
    */
   Set<String> enableTTs(Map<String, Object> toEnable, int totalTargetEnabled, String clusterId) throws Exception;

   /**
    * Disable a candidate set of VMs for a target cluster
    * 
    * @param toDisable A set of VM ids that must belong to the same cluster
    * @param totalTargetEnabled The total target number of Task Tracker VMs that should be enabled for the cluster on completion
    * @param clusterId The cluster that the VM ids belong to
    * @return The Set of VM ids that was actually disabled, or null
    * @throws Exception
    */
   Set<String> disableTTs(Set<String> toDisable, int totalTargetEnabled, String clusterId) throws Exception;

   /**
    * Disable a candidate set of VMs for a target cluster
    * Optional context for each VM is passed through as an object value in the toDisable map
    * 
    * @param toDisable A map of VM ids that must belong to the same cluster, along with optional Object context
    * @param totalTargetEnabled The total target number of Task Tracker VMs that should be enabled for the cluster on completion
    * @param clusterId The cluster that the VM ids belong to
    * @return The Set of VM ids that was actually disabled, or null
    * @throws Exception
    */
   Set<String> disableTTs(Map<String, Object> toDisable, int totalTargetEnabled, String clusterId) throws Exception;

   /**
    * Returns VM ids of active Task Trackers for a given cluster Id
    * 
    * @param clusterId The id of the cluster
    * @return VM ids of active Task Trackers, or null
    * @throws Exception
    */
   Set<String> getActiveTTs(String clusterId) throws Exception;
}
