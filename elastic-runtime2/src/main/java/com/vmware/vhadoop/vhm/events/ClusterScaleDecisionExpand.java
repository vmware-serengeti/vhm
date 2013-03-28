package com.vmware.vhadoop.vhm.events;

import java.util.Set;

public class ClusterScaleDecisionExpand extends AbstractClusterScaleDecision {

   Set<String> _vmIds;
   
   public ClusterScaleDecisionExpand(String clusterId, Set<String> vmIds) {
      super(clusterId);
      _vmIds = vmIds;
   }
   
   /* TODO: Update vmIds when ClusterStateChangeEvents come in */
}
