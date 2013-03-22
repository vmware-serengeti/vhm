package com.vmware.vhadoop.vhm.strategy;

import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;

public class DumbEDPolicy implements EDPolicy {
   private static final Logger _log = Logger.getLogger(DumbEDPolicy.class.getName());

   final VCActions _vcActions;
   
   public DumbEDPolicy(VCActions vcActions) {
      _vcActions = vcActions;
   }

   @Override
   public void enableTTs(Set<String> toEnable, int totalTargetEnabled, String clusterId, ClusterMap cluster)
         throws Exception {
      for (String vmMoRef : toEnable) {
         _log.info("DumbEDPolicy enabling TT for "+vmMoRef);
         _vcActions.changeVMPowerState(vmMoRef, true);
      }
      /* TODO: Should block and check success */
   }
   

   @Override
   public void disableTTs(Set<String> toDisable, int totalTargetEnabled, String clusterId, ClusterMap cluster)
         throws Exception {
      for (String vmMoRef : toDisable) {
         _log.info("DumbEDPolicy disabling TT for "+vmMoRef);
         _vcActions.changeVMPowerState(vmMoRef, false);
      }
      /* TODO: Should block and check success */
   }

}
