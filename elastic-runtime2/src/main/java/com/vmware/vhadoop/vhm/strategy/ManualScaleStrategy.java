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
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;

import java.util.*;
import java.util.logging.Logger;

public class ManualScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   private static final Logger _log = Logger.getLogger(ManualScaleStrategy.class.getName());
   final VMChooser _vmChooser;
   final EDPolicy _enableDisablePolicy;
   
   public static final String MANUAL_SCALE_STRATEGY_KEY = "manual";
   public static final int TARGET_SIZE_UNLIMITED = -1;
   
   public ManualScaleStrategy(VMChooser vmChooser, EDPolicy edPolicy) {
      _vmChooser = vmChooser;
      _enableDisablePolicy = edPolicy;
   }
   
   @Override
   public void initialize(ClusterMapReader parent) {
      super.initialize(parent);
      _vmChooser.initialize(parent);
      _enableDisablePolicy.initialize(parent);
   }

   @Override
   public String getKey() {
      return MANUAL_SCALE_STRATEGY_KEY;
   }

   class CallableStrategy extends ClusterScaleOperation {
      final Set<ClusterScaleEvent> _events;
      
      public CallableStrategy(Set<ClusterScaleEvent> events) {
         _events = events;
         initialize(ManualScaleStrategy.this);
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
            SerengetiLimitInstruction limitEvent = (SerengetiLimitInstruction)event;
            int targetSize = 0;
            int delta = 0;
            String clusterId = null;
            Set<String> vmsToED;
            ClusterMap clusterMap = getAndReadLockClusterMap(); 
            try {
               String clusterFolder = limitEvent.getClusterFolderName();
               clusterId = clusterMap.getClusterIdForFolder(clusterFolder);
               if (clusterId != null) {
                  Set<String> poweredOffVmList = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, false);
                  int poweredOffVms = (poweredOffVmList == null) ? 0 : poweredOffVmList.size();
                  Set<String> poweredOnVmList = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, true);
                  int poweredOnVms = (poweredOnVmList == null) ? 0 : poweredOnVmList.size();
                  if (limitEvent.getToSize() == TARGET_SIZE_UNLIMITED) {
                     targetSize = poweredOnVms + poweredOffVms;
                     delta = poweredOffVms;
                  } else {
                     targetSize = limitEvent.getToSize();
                     delta = targetSize - poweredOnVms;
                  }
                  returnEvent = new ClusterScaleDecision(clusterId);
               } else {
                  tlStatus.registerTaskFailed(false, "Unknown clusterId for Cluster Folder "+clusterFolder);
                  /* delta == 0, so don't do anything */
               }
            } finally {
               unlockClusterMap(clusterMap);
            } 
            if (delta > 0) {
               vmsToED = _vmChooser.chooseVMsToEnable(clusterId, delta);
               limitEvent.reportProgress(10, null);
               if ((vmsToED != null) && !vmsToED.isEmpty()) {
                  _enableDisablePolicy.enableTTs(vmsToED, targetSize, clusterId);
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
               if ((vmsToED != null) && !vmsToED.isEmpty()) {
                  _enableDisablePolicy.disableTTs(vmsToED, targetSize, clusterId);
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

   @SuppressWarnings("unchecked")
   @Override
   public Class<? extends ClusterScaleEvent>[] getScaleEventTypesHandled() {
      return new Class[]{SerengetiLimitInstruction.class};
   }
   
   @Override
   public String toString() {
      return getKey();
   }
}
