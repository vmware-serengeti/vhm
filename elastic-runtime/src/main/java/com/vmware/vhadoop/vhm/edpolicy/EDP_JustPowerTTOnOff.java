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
import com.vmware.vhadoop.external.VCActions;

public class EDP_JustPowerTTOnOff extends AbstractEDP {
   public EDP_JustPowerTTOnOff(VCActions vc) {
      super(vc);
   }

   /* TODO: We currently don't track any kind of success/failure in these methods */

   @Override
   /* Currently blocking */
   public boolean enableTTs(VMDTO[] toEnable, int totalTargetEnabled, HadoopCluster cluster) throws Exception {
      return powerOnVMs(toEnable);
   }

   @Override
   /* Currently blocking */
   public boolean disableTTs(VMDTO[] toDisable, int totalTargetEnabled, HadoopCluster cluster) throws Exception {
      return powerOffVMs(toDisable);
   }
   
   @Override
   public boolean testForSuccess(VMDTO[] vms, boolean assertEnabled) {
      return testForPowerState(vms, assertEnabled);
   }
}
