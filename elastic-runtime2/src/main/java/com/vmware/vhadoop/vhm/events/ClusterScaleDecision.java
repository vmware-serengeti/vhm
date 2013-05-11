package com.vmware.vhadoop.vhm.events;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public class ClusterScaleDecision extends AbstractNotificationEvent implements ClusterScaleCompletionEvent {
   String _clusterId;
   Map<String, Decision> _decisions;
   
   public ClusterScaleDecision(String clusterId) {
      super(false, true);
      _clusterId = clusterId;
      _decisions = new HashMap<String, Decision>();
   }

   @Override
   public String getClusterId() {
      return _clusterId;
   }
   
   public void addDecision(String vmId, Decision decision) {
      _decisions.put(vmId, decision);
   }

   public void addDecision(Set<String> vmIds, Decision decision) {
      for (String vmId : vmIds) {
         _decisions.put(vmId, decision);
      }
   }

   @Override
   public Decision getDecisionForVM(String vmId) {
      return _decisions.get(vmId);
   }
}
