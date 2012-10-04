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

import java.util.logging.Level;

import com.vmware.vhadoop.external.HadoopActions;
import com.vmware.vhadoop.external.HadoopCluster;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActions;
import com.vmware.vhadoop.util.CompoundStatus;

public class EDP_DeRecommissionTTs extends AbstractEDP {
    private final String _className = this.getClass().getName();
	HadoopActions _hc;
	
	public EDP_DeRecommissionTTs(VCActions vc, HadoopActions hc) {
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
	public CompoundStatus enableTTs(VMDTO[] toEnable, int totalTargetEnabled, HadoopCluster cluster) throws Exception {
	   CompoundStatus finalStatus = new CompoundStatus(_className+" enable");
       finalStatus.addStatus(_hc.recommissionTTs(getHostNamesForVMs(toEnable), cluster));
       /* TODO: Should powerOn be conditional */
       finalStatus.addStatus(powerOnVMs(toEnable));
	   /* TODO: Blocking is conditional on success. That's probably right? */
	   if (finalStatus.getFailedTaskCount() == 0) {
	      finalStatus.addStatus(_hc.checkTargetTTsSuccess("Recommission", getHostNamesForVMs(toEnable), totalTargetEnabled, cluster));
	   }
	   return finalStatus;
	}

	@Override
	/* Blocks until completed */
	public CompoundStatus disableTTs(VMDTO[] toDisable, int totalTargetEnabled, HadoopCluster cluster) throws Exception {
       CompoundStatus finalStatus = new CompoundStatus(_className+" disable");
	   finalStatus.addStatus(_hc.decommissionTTs(getHostNamesForVMs(toDisable), cluster));
	   
	   if (finalStatus.getFailedTaskCount() > 0) {
	      _log.log(Level.SEVERE, "Decommission failed. See vhm.log and/or vhm.xml for details");
	   } else {
	      finalStatus.addStatus(_hc.checkTargetTTsSuccess("Decommission", getHostNamesForVMs(toDisable), totalTargetEnabled, cluster));
		  if (finalStatus.getFailedTaskCount() > 0) {
			  _log.log(Level.SEVERE, "Decommission check failed. See vhm.log and/or vhm.xml for details");
		  }
	   }
	   finalStatus.addStatus(powerOffVMs(toDisable));
	   
	   return finalStatus;
	}
}
