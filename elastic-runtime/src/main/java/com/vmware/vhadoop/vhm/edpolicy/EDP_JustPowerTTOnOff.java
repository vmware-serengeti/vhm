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
