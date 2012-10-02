/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
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

package com.vmware.vhadoop.vhm.edpolicy;

import com.vmware.vhadoop.external.HadoopCluster;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public interface EnableDisableTTPolicy {
   
   /* Caller should expect this to block */
   boolean enableTTs(VMDTO[] toEnable, int totalTargetEnabled, HadoopCluster cluster) throws Exception;

   /* Caller should expect this to block */
   boolean disableTTs(VMDTO[] toDisable, int totalTargetEnabled, HadoopCluster cluster) throws Exception;

   /* All task trackers on all hosts being managed by the VHM are the input values (in no particular order)
    * The output is the enabled/disabled state of all the VMs organized by host */
   TTStatesForHost[] getStateForTTs(VMDTO[] ttVMs) throws Exception;

   boolean testForSuccess(VMDTO[] vms, boolean assertEnabled);

}
