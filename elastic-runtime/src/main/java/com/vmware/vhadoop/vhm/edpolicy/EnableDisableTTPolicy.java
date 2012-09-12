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
