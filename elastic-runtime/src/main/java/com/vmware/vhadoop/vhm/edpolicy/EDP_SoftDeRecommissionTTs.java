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

import com.vmware.vhadoop.external.HadoopActions;
import com.vmware.vhadoop.external.HadoopCluster;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActions;

public class EDP_SoftDeRecommissionTTs extends AbstractEDP {
	HadoopActions _hc;
	
	public EDP_SoftDeRecommissionTTs(VCActions vc, HadoopActions hc) {
	   super(vc);
		_hc = hc;
	}
	
	private String[] getHostNamesForVMs(VMDTO[] vms) {
	   String[] result = new String[vms.length];
	   for (int i=0; i<vms.length; i++) {
	      result[i] = getVC().getVMHostname(vms[i]);
	   }
	   return result;
	}

	@Override
	public boolean enableTTs(VMDTO[] toEnable, int totalTargetEnabled, HadoopCluster cluster) throws Exception {
       boolean recomSuccess = _hc.recommissionTTs(getHostNamesForVMs(toEnable), cluster);
       /* TODO: Should powerOn be conditional */
	   boolean combinedResult = recomSuccess & powerOnVMs(toEnable);
	   /* TODO: Blocking is conditional on success. That's probably right? */
	   if (combinedResult) {
	      return _hc.checkTargetTTsSuccess("Recommission", getHostNamesForVMs(toEnable), totalTargetEnabled, cluster);
	   }
	   return combinedResult;
	}

	@Override
	/* Blocks until completed */
	public boolean disableTTs(VMDTO[] toDisable, int totalTargetEnabled, HadoopCluster cluster) throws Exception {
	   boolean decomSuccess = _hc.decommissionTTs(getHostNamesForVMs(toDisable), cluster);
	   boolean checkSuccess = false; 
	   if (decomSuccess) {
		      checkSuccess = _hc.checkTargetTTsSuccess("Decommission", getHostNamesForVMs(toDisable), totalTargetEnabled, cluster);
	   }
       /* TODO: Should shutdownGuest be conditional? */
	   return checkSuccess & shutdownGuests(toDisable);
	}

   @Override
   public boolean testForSuccess(VMDTO[] vms, boolean assertEnabled) {
      if (!assertEnabled) {
         /* In the case where we decommissions, just check that the VMs are powered off for now */
         return testForPowerState(vms, assertEnabled);
      } else {
         /* If we got here, it means that enableTTs returned true, in which case the TTs should all be enabled */
         /* TODO: While enableTTs is blocking on the check script, what else can we test for here? */
         return true;
      }
   }
}
