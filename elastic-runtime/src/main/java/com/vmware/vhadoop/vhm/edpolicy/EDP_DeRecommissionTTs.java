package com.vmware.vhadoop.vhm.edpolicy;

import com.vmware.vhadoop.external.HadoopActions;
import com.vmware.vhadoop.external.HadoopCluster;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActions;

public class EDP_DeRecommissionTTs extends AbstractEDP {
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
       /* TODO: Should powerOff be conditional? */
	   return checkSuccess & powerOffVMs(toDisable);
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
