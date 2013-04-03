package com.vmware.vhadoop.vhm.events;

import java.util.HashMap;
import java.util.Map;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public class ClusterScaleDecision extends AbstractNotificationEvent implements ClusterScaleCompletionEvent {
   String _clusterId;
   Map<String, Decision> _decisions;
   Runnable _completionCode;
   
   public ClusterScaleDecision(String clusterId) {
      super(false, false);
      _clusterId = clusterId;
      _decisions = new HashMap<String, Decision>();
   }

   @Override
   public String getClusterId() {
      return _clusterId;
   }
   
   public void addDecision(String hostId, Decision decision) {
      _decisions.put(hostId, decision);
   }

   @Override
   public Decision getDecisionForHost(String hostId) {
      return _decisions.get(hostId);
   }
}
