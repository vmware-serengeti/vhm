package com.vmware.vhadoop.vhm.strategy;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.CompoundStatus.TaskStatus;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
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
   public void registerClusterMapAccess(ClusterMapAccess access, ThreadLocalCompoundStatus tlcs) {
      super.registerClusterMapAccess(access, tlcs);
      _vmChooser.registerClusterMapAccess(access, tlcs);
      _enableDisablePolicy.registerClusterMapAccess(access, tlcs);
   }

   @Override
   public String getName() {
      return MANUAL_SCALE_STRATEGY_KEY;
   }

   class CallableStrategy extends ClusterScaleOperation {
      final Set<ClusterScaleEvent> _events;
      
      public CallableStrategy(Set<ClusterScaleEvent> events) {
         super(cloneClusterMapAccess(), getThreadLocalCompoundStatus());
         _events = events;
      }

      @Override
      public ClusterScaleCompletionEvent localCall() throws Exception {
         CompoundStatus tlStatus = getCompoundStatus();
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
               limitEvent.reportProgress(10, null);
               if (vmsToED != null) {
                  _enableDisablePolicy.enableTTs(vmsToED, limitEvent.getToSize(), clusterId);
                  limitEvent.reportProgress(30, null);
                  returnEvent.addDecision(vmsToED, ClusterScaleCompletionEvent.ENABLE);
                  if (tlStatus.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_ON_STATUS_KEY})) {
                     blockOnPowerStateChange(vmsToED, true, 120000);
                  }
                  limitEvent.reportProgress(90, null);
               }
            } else if (delta < 0) {
               vmsToED = _vmChooser.chooseVMsToDisable(clusterId, delta);
               limitEvent.reportProgress(10, null);
               if (vmsToED != null) {
                  _enableDisablePolicy.disableTTs(vmsToED, limitEvent.getToSize(), clusterId);
                  limitEvent.reportProgress(30, null);
                  returnEvent.addDecision(vmsToED, ClusterScaleCompletionEvent.DISABLE);
                  if (tlStatus.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_OFF_STATUS_KEY})) {
                     blockOnPowerStateChange(vmsToED, false, 120000);
                  }
                  limitEvent.reportProgress(90, null);
               }
            }
            if (tlStatus.getFailedTaskCount() == 0) {
               limitEvent.reportCompletion();
            } else {
               TaskStatus firstError = tlStatus.getFirstFailure();
               if (tlStatus.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_ON_STATUS_KEY, 
                     VCActions.VC_POWER_OFF_STATUS_KEY, 
                     ClusterMapReader.POWER_STATE_CHANGE_STATUS_KEY})) {
                  limitEvent.reportError(firstError.getMessage() + " however, powering on/off VMs succeeded;");
               } else {
                  limitEvent.reportError(firstError.getMessage());
               }
            }
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
