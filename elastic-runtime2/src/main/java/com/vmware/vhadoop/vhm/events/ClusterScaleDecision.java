package com.vmware.vhadoop.vhm.events;

import java.util.HashMap;
import java.util.Map;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public class ClusterScaleDecision extends AbstractNotificationEvent implements ClusterScaleCompletionEvent {
   String _clusterId;
   Map<String, DecisionAndOutcome> _decisions;
   Runnable _completionCode;
   boolean _outcomeComplete;
   boolean _spawnChildThread;

   private class DecisionAndOutcome {
      DecisionAndOutcome(Decision decision) {
         _decision = decision;
      }
      Decision _decision;
      boolean _outcome;
   }
   
   public ClusterScaleDecision(String clusterId) {
      super(false, false);
      _clusterId = clusterId;
      _decisions = new HashMap<String, DecisionAndOutcome>();
   }

   @Override
   public String getClusterId() {
      return _clusterId;
   }
   
   public void addDecision(String vmId, Decision decision) {
      _decisions.put(vmId, new DecisionAndOutcome(decision));
   }
   
   public void setOutcomeComplete(String vmId) {
      DecisionAndOutcome dao = _decisions.get(vmId);
      if (dao != null) {
         dao._outcome = true;
      }
   }

   public void setOutcomeCompleteBlock(Runnable completionCode, boolean spawnChildThread) {
      _completionCode = completionCode;
      _spawnChildThread = spawnChildThread;
   }
   
   /* Note this will be run in the VHM main thread as part of an update with the ClusterMap locked, 
    * so anything that may block for any time must be invoked in a separate thread */
   public boolean runOutcomeCompleteBlock() {
      if (_outcomeComplete && (_completionCode != null)) {
         if (_spawnChildThread) {
            new Thread(_completionCode).start();
         } else {
            _completionCode.run();
         }
         _completionCode = null;
         return true;
      }
      return false;
   }

   @Override
   public Decision getDecisionForVM(String vmId) {
      DecisionAndOutcome dao = _decisions.get(vmId);
      return (dao != null) ? dao._decision : null;
   }

   @Override
   public boolean getOutcomeCompleteForVM(String vmId) {
      DecisionAndOutcome dao = _decisions.get(vmId);
      return (dao != null) ? dao._outcome : null;
   }

   @Override
   public boolean getOutcomeCompleteForAllVMs() {
      for (String vmId : _decisions.keySet()) {
         if (!getOutcomeCompleteForVM(vmId)) {
            return false;
         }
      }
      return (_outcomeComplete = true);
   }
   
}
