package com.vmware.vhadoop.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.EDPolicy;
import com.vmware.vhadoop.api.vhm.VCActions;

public class DumbEDPolicy implements EDPolicy {
   final VCActions _vcActions;
   
   public DumbEDPolicy(VCActions vcActions) {
      _vcActions = vcActions;
   }

   @Override
   public void enableTTs(Set<String> toEnable, int totalTargetEnabled, String clusterId, ClusterMap cluster)
         throws Exception {
      for (String vmMoRef : toEnable) {
         _vcActions.changeVMPowerState(vmMoRef, true);
      }
      /* TODO: Should block and check success */
   }
   

   @Override
   public void disableTTs(Set<String> toDisable, int totalTargetEnabled, String clusterId, ClusterMap cluster)
         throws Exception {
      for (String vmMoRef : toDisable) {
         _vcActions.changeVMPowerState(vmMoRef, false);
      }
      /* TODO: Should block and check success */
   }

}
