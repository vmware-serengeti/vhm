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

import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public interface VMChooser extends ClusterMapReader {
   /**
    * Selects VMs to enable from the specified cluster. The logic determining which VMs is provided by implementors.
    * Synonymous with chooseVMsToEnable(listVMsForCluster(clusterId), delta)
    * @param clusterId - the target cluster
    * @param delta - the number of VMs to enable
    * @return - set of VM ids to enable
    */
   Set<String> chooseVMsToEnable(String clusterId, int delta);

   /**
    * Selects VMs to disable from the specified cluster. The logic determining which VMs is provided by implementors.
    * Synonymous with chooseVMsToDisable(listVMsForCluster(clusterId), delta)
    * @param clusterId - the target cluster
    * @param delta - the number of VMs to disable
    * @return - set of VM ids to disable
    */
   Set<String> chooseVMsToDisable(String clusterId, int delta);

   /**
    * Selects VMs to enable from the specified set. The logic determining which VMs is provided by implementors.
    * @param candidates - the candidate VMs
    * @param delta - the number of VMs to enable
    * @return - set of VM ids to enable (subset of candidates)
    */
//   Set<String> chooseVMsToEnable(Set<String> candidates, int delta);

   /**
    * Selects VMs to disable from the specified set. The logic determining which VMs is provided by implementors.
    * @param candidates - the candidate VMs
    * @param delta - the number of VMs to disable
    * @return - set of VM ids to disable (subset of candidates)
    */
//   Set<String> chooseVMsToDisable(Set<String> candidates, int delta);

   /**
    * Selects a single VM out of the specified set to enable. All the candidates must be on the same host.
    * @param candidates - the candidate VMs
    * @return - set of VM ids to enable (subset of candidates)
    */
   String chooseVMToEnableOnHost(Set<String> candidates);

   /**
    * Selects a single VM out of the specified set to disable. All the candidates must be on the same host.
    * @param candidates - the candidate VMs
    * @return - set of VM ids to disable (subset of candidates)
    */
   String chooseVMToDisableOnHost(Set<String> candidates);
}
