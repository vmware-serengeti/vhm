package com.vmware.vhadoop.vhm.strategy;

import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class DumbEDPolicy extends AbstractClusterMapReader implements EDPolicy {
   private static final Logger _log = Logger.getLogger(DumbEDPolicy.class.getName());

   final VCActions _vcActions;
   
   public DumbEDPolicy(VCActions vcActions) {
      _vcActions = vcActions;
   }

   @Override
   public void enableTTs(Set<String> toEnable, int totalTargetEnabled, String clusterId)
         throws Exception {
      _log.info("About to enable "+toEnable.size()+" TTs to get to target of "+totalTargetEnabled+" for cluster "+clusterId+"...");
      _vcActions.changeVMPowerState(toEnable, true);
   }
   

   @Override
   public void disableTTs(Set<String> toDisable, int totalTargetEnabled, String clusterId)
         throws Exception {
      _log.info("About to disable "+toDisable.size()+" TTs to get to target of "+totalTargetEnabled+" for cluster "+clusterId+"...");
      _vcActions.changeVMPowerState(toDisable, false);
   }

}
