package com.vmware.vhadoop.vhm.events;

import java.util.Set;

public class ClusterScaleDecisionShrink extends AbstractClusterScaleDecision {

   Set<String> _vmIds;
   
   public ClusterScaleDecisionShrink(String clusterId, Set<String> vmIds) {
      super(clusterId);
      _vmIds = vmIds;
   }
   
   /* TODO: Update vmIds when ClusterStateChangeEvents come in */

}
