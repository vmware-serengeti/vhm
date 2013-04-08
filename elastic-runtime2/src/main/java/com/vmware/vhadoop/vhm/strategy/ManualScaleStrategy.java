package com.vmware.vhadoop.vhm.strategy;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;

import java.util.*;

public class ManualScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   final VMChooser _vmChooser;
   final EDPolicy _enableDisablePolicy;
   
   public static final String MANUAL_SCALE_STRATEGY_KEY = "manual";
   
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
      return MANUAL_SCALE_STRATEGY_KEY;
   }

   class CallableStrategy extends ClusterScaleOperation {
      final Set<ClusterScaleEvent> _events;
      
      public CallableStrategy(Set<ClusterScaleEvent> events) {
         super(cloneClusterMapAccess());
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
                  returnEvent.addDecision(vmsToED, ClusterScaleCompletionEvent.ENABLE);
                  blockOnPowerStateChange(vmsToED, true, 120000);
               }
            } else if (delta < 0) {
               vmsToED = _vmChooser.chooseVMsToDisable(clusterId, delta);
               if (vmsToED != null) {
                  _enableDisablePolicy.disableTTs(vmsToED, limitEvent.getToSize(), clusterId);
                  returnEvent.addDecision(vmsToED, ClusterScaleCompletionEvent.DISABLE);
                  blockOnPowerStateChange(vmsToED, false, 120000);
               }
            }
            limitEvent.reportCompletion();
         } else {
            throw new RuntimeException("Manual scale strategy event should be of type SerengetiLimitInstruction");
         }
         return returnEvent;
      }

   }

   @Override
   public ClusterScaleOperation getClusterScaleOperation(String clusterId, Set<ClusterScaleEvent> events, ScaleStrategyContext context) {
      return new CallableStrategy(events);
   }

   @Override
   public Class<? extends ScaleStrategyContext> getStrategyContextType() {
      return null;
   }
}
