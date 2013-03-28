package com.vmware.vhadoop.vhm.strategy;

import java.util.concurrent.Callable;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecisionExpand;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecisionShrink;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;

import java.util.*;

public class ManualScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   final VMChooser _vmChooser;
   final EDPolicy _enableDisablePolicy;
   
   public ManualScaleStrategy(VMChooser vmChooser, EDPolicy edPolicy) {
      _vmChooser = vmChooser;
      _enableDisablePolicy = edPolicy;
   }

   class CallableStrategy implements Callable<ClusterScaleCompletionEvent> {
      final Set<ClusterScaleEvent> _events;
      
      public CallableStrategy(Set<ClusterScaleEvent> events) {
         _events = events;
      }

      @Override
      public ClusterScaleCompletionEvent call() throws Exception {
         ClusterScaleCompletionEvent returnEvent = null;
         if (_events.size() != 1) {
            throw new RuntimeException("Manual scale strategy should only have one SerengetiLimitInstruction");
         }
         ClusterScaleEvent event = _events.iterator().next();
         if (event instanceof SerengetiLimitInstruction) {
            SerengetiLimitInstruction limitEvent = (SerengetiLimitInstruction)event;
            ClusterMap clusterMap = getAndReadLockClusterMap();
            String clusterId = clusterMap.getClusterIdForFolder(limitEvent.getClusterFolderName());
            int poweredOnVms = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, true).size();
            int delta = limitEvent.getToSize() - poweredOnVms;
            Set<String> vmsToED;
            if (delta > 0) {
               vmsToED = _vmChooser.chooseVMsToEnable(clusterId, clusterMap, delta);
               int targetSize = limitEvent.getToSize();
               if (vmsToED != null) {
                  _enableDisablePolicy.enableTTs(vmsToED, targetSize, clusterId, clusterMap);
               }
               returnEvent = new ClusterScaleDecisionExpand(clusterId, vmsToED);
            } else if (delta < 0) {
               vmsToED = _vmChooser.chooseVMsToDisable(clusterId, clusterMap, delta);
               int targetSize = limitEvent.getToSize();
               if (vmsToED != null) {
                  _enableDisablePolicy.disableTTs(vmsToED, limitEvent.getToSize(), clusterId, clusterMap);
               }
               returnEvent = new ClusterScaleDecisionShrink(clusterId, vmsToED);
            }
            limitEvent.reportCompletion();
         } else {
            throw new RuntimeException("Manual scale strategy event should be of type SerengetiLimitInstruction");
         }
         return returnEvent;
      }
      
   }

   @Override
   public Callable<ClusterScaleCompletionEvent> getCallable(Set<ClusterScaleEvent> events) {
      return new CallableStrategy(events);
   }
}
