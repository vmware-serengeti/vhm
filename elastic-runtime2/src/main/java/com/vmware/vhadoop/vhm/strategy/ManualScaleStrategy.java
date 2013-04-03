package com.vmware.vhadoop.vhm.strategy;

import java.util.concurrent.Callable;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent.Decision;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;

import java.util.*;

public class ManualScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   final VMChooser _vmChooser;
   final EDPolicy _enableDisablePolicy;
   
   public ManualScaleStrategy(VMChooser vmChooser, EDPolicy edPolicy) {
      _vmChooser = vmChooser;
      _enableDisablePolicy = edPolicy;
   }
   
   @Override
   public void registerClusterMapAccess(ClusterMapAccess access) {
      super.registerClusterMapAccess(access);
      _vmChooser.registerClusterMapAccess(access);
      _enableDisablePolicy.registerClusterMapAccess(access);
   }

   @Override
   public String getName() {
      return "manual";     /* TODO: Needs to be consistent with key put in VM extraInfo */
   }

   class CallableStrategy implements Callable<ClusterScaleCompletionEvent> {
      final Set<ClusterScaleEvent> _events;
      
      public CallableStrategy(Set<ClusterScaleEvent> events) {
         _events = events;
      }

      @Override
      public ClusterScaleCompletionEvent call() throws Exception {
         ClusterScaleDecision returnEvent = null;
         if (_events.size() != 1) {
            throw new RuntimeException("Manual scale strategy should only have one SerengetiLimitInstruction");
         }
         ClusterScaleEvent event = _events.iterator().next();
         if (event instanceof SerengetiLimitInstruction) {
            final SerengetiLimitInstruction limitEvent = (SerengetiLimitInstruction)event;
            ClusterMap clusterMap = getAndReadLockClusterMap();
            String clusterId = clusterMap.getClusterIdForFolder(limitEvent.getClusterFolderName());
            int poweredOnVms = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, true).size();
            unlockClusterMap(clusterMap);
            int delta = limitEvent.getToSize() - poweredOnVms;
            Set<String> vmsToED;
            returnEvent = new ClusterScaleDecision(clusterId);
            if (delta > 0) {
               vmsToED = _vmChooser.chooseVMsToEnable(clusterId, delta);
               if (vmsToED != null) {
                  _enableDisablePolicy.enableTTs(vmsToED, limitEvent.getToSize(), clusterId);
                  returnEvent = createClusterScaleCompletionEventFromVMs(clusterId, vmsToED, ClusterScaleCompletionEvent.EXPAND);
                  blockOnPowerStateChange(vmsToED, true, 120000);
               }
            } else if (delta < 0) {
               vmsToED = _vmChooser.chooseVMsToDisable(clusterId, delta);
               if (vmsToED != null) {
                  _enableDisablePolicy.disableTTs(vmsToED, limitEvent.getToSize(), clusterId);
                  returnEvent = createClusterScaleCompletionEventFromVMs(clusterId, vmsToED, ClusterScaleCompletionEvent.SHRINK);
                  blockOnPowerStateChange(vmsToED, false, 120000);
               }
            }
            limitEvent.reportCompletion();
         } else {
            throw new RuntimeException("Manual scale strategy event should be of type SerengetiLimitInstruction");
         }
         return returnEvent;
      }

      private ClusterScaleDecision createClusterScaleCompletionEventFromVMs(String clusterId, Set<String> vmsToED, Decision decision) {
         ClusterScaleDecision result = null;
         ClusterMap clusterMap = getAndReadLockClusterMap();
         Map<String, String> hostIds = clusterMap.getHostIdsForVMs(vmsToED);
         unlockClusterMap(clusterMap);
         Set<String> uniqueHostIds = new HashSet<String>(hostIds.values());
         if (uniqueHostIds.size() > 0) {
            result = new ClusterScaleDecision(clusterId);
            for (String hostId : uniqueHostIds) {
               result.addDecision(hostId, decision);
            }
         }
         return result;
      }
      
   }

   @Override
   public Callable<ClusterScaleCompletionEvent> getCallable(Set<ClusterScaleEvent> events) {
      return new CallableStrategy(events);
   }
}
