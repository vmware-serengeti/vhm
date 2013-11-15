/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

package com.vmware.vhadoop.vhm.strategy;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser.RankedVM;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.CompoundStatus.TaskStatus;
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction.SerengetiLimitAction;

public class ManualScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   private static final Logger _log = Logger.getLogger(ManualScaleStrategy.class.getName());
   private final EDPolicy _enableDisablePolicy;
   private VMChooserCallback _vmChooserCallback;

   public static final String MANUAL_SCALE_STRATEGY_KEY = "manual";

   public ManualScaleStrategy(EDPolicy edPolicy) {
      _enableDisablePolicy = edPolicy;
   }

   @Override
   public void initialize(ClusterMapReader parent) {
      super.initialize(parent);
      _enableDisablePolicy.initialize(parent);
   }

   @Override
   public void setVMChooserCallback(final VMChooserCallback callback) {
      _vmChooserCallback = callback;
   }

   @Override
   public String getKey() {
      return MANUAL_SCALE_STRATEGY_KEY;
   }

   private Set<String> chooseVMsForTargetPowerState(String clusterId, int delta, Set<String> candidateVmIds, boolean targetPowerState) {
      Set<RankedVM> combination = null;
      for (VMChooser vmChooser : _vmChooserCallback.getVMChoosers()) {

         Set<RankedVM> rankedVMs = targetPowerState ?
               vmChooser.rankVMsToEnable(clusterId, candidateVmIds) :
               vmChooser.rankVMsToDisable(clusterId, candidateVmIds);

         if (rankedVMs != null) {
            combination = RankedVM.combine(combination, rankedVMs);
         }
      }
      return RankedVM.selectLowestRankedIds(combination, Math.abs(delta));
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
            ClusterMap clusterMap = null;
            Set<String> poweredOffVmIds = null;
            Set<String> poweredOnVmIds = null;
            int numPoweredOff = 0;
            int numPoweredOn = 0;
            try {
               clusterMap = getAndReadLockClusterMap();
               String clusterFolder = limitEvent.getClusterFolderName();
               clusterId = clusterMap.getClusterIdForFolder(clusterFolder);
               if (clusterId != null) {
                  poweredOffVmIds = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, false);
                  numPoweredOff = (poweredOffVmIds == null) ? 0 : poweredOffVmIds.size();
                  poweredOnVmIds = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, true);
                  numPoweredOn = (poweredOnVmIds == null) ? 0 : poweredOnVmIds.size();
                  if (limitEvent.getAction().equals(SerengetiLimitAction.actionUnlimit)) {
                     targetSize = numPoweredOn + numPoweredOff;
                     delta = numPoweredOff;
                  } else {
                     targetSize = limitEvent.getToSize();
                     delta = targetSize - numPoweredOn;
                  }
                  _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: handling manual elasticity command from Serengeti to set number of enabled compute nodes to " + targetSize);
                  returnEvent = new ClusterScaleDecision(clusterId);
               } else {
                  tlStatus.registerTaskFailed(false, "Unknown clusterId for Cluster Folder "+clusterFolder);
                  /* delta == 0, so don't do anything */
               }
            } finally {
               unlockClusterMap(clusterMap);
            }
            Set<String> unresponsiveVmIds = null;
            if (delta > 0) {
               vmsToED = chooseVMsForTargetPowerState(clusterId, delta, poweredOffVmIds, true);
               limitEvent.reportProgress(10, null);
               if ((vmsToED != null) && !vmsToED.isEmpty()) {
                  /* Note that this returns successfully enabled VM IDs from the input set of VMs*/
                  Set<String> enabledTTs = _enableDisablePolicy.enableTTs(vmsToED, targetSize, clusterId);
                  if (enabledTTs != null) {
                     _log.fine("Enabled TTs: "+enabledTTs);
                     unresponsiveVmIds = diffIds(vmsToED, enabledTTs);
                     limitEvent.reportProgress(30, null);
                     returnEvent.addDecision(enabledTTs, ClusterScaleCompletionEvent.ENABLE);
                     if (tlStatus.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_ON_STATUS_KEY})) {
                        blockOnPowerStateChange(enabledTTs, true, 120000);
                     }
                     limitEvent.reportProgress(90, null);
                  } else {
                     unresponsiveVmIds = vmsToED;
                     tlStatus.registerTaskFailed(false, "no task trackers were enabled successfully");
                  }
               }
            } else if (delta < 0) {
               vmsToED = chooseVMsForTargetPowerState(clusterId, delta, poweredOnVmIds, false);
               limitEvent.reportProgress(10, null);
               if ((vmsToED != null) && !vmsToED.isEmpty()) {
                  /* Note that this returns disabled VM IDs for the cluster */
                  Set<String> disabledTTs = _enableDisablePolicy.disableTTs(vmsToED, targetSize, clusterId);
                  if (disabledTTs != null) {
                     _log.fine("Disabled TTs: "+disabledTTs);
                     unresponsiveVmIds = diffIds(vmsToED, disabledTTs);
                     limitEvent.reportProgress(30, null);
                     returnEvent.addDecision(disabledTTs, ClusterScaleCompletionEvent.DISABLE);
                     if (tlStatus.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_OFF_STATUS_KEY})) {
                        blockOnPowerStateChange(disabledTTs, false, 120000);
                     }
                     limitEvent.reportProgress(90, null);
                  } else {
                     unresponsiveVmIds = vmsToED;
                     tlStatus.registerTaskFailed(false, "no task trackers were disabled successfully");
                  }
               }
            }
            if (tlStatus.getFailedTaskCount() == 0) {
               limitEvent.reportCompletion();
            } else {
               if (unresponsiveVmIds != null) {
                  for (String uninitializedVmId : unresponsiveVmIds) {
                     _log.warning("<%C"+event.getClusterId()+"%C>: <%V"+uninitializedVmId+"%V> - did not successfully respond in a reasonable time");
                  }
               }
               TaskStatus firstGeneralError = tlStatus.getFirstFailure();
               if (firstGeneralError != null) {
                  if (tlStatus.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_ON_STATUS_KEY,
                     VCActions.VC_POWER_OFF_STATUS_KEY,
                     ClusterMapReader.POWER_STATE_CHANGE_STATUS_KEY})) {
                     limitEvent.reportError(firstGeneralError.getMessage() + " however, powering on/off VMs succeeded");
                  } else {
                     limitEvent.reportError(firstGeneralError.getMessage());
                  }
               }
            }
         } else {
            throw new RuntimeException("Manual scale strategy event should be of type SerengetiLimitInstruction");
         }
         return returnEvent;
      }

      private Set<String> diffIds(Set<String> vmIdsInstructed, Set<String> vmIdsInitialized) {
         Set<String> result = new HashSet<String>(vmIdsInstructed);
         result.removeAll(vmIdsInitialized);
         if (result.size() == 0) {
            return null;
         }
         return result;
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
