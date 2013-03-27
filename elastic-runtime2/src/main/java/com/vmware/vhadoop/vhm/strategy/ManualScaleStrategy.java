package com.vmware.vhadoop.vhm.strategy;

import java.util.concurrent.Callable;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;
import com.vmware.vhadoop.vhm.events.SerengetiLimitEvent;

import java.util.*;

public class ManualScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   final VMChooser _vmChooser;
   final EDPolicy _enableDisablePolicy;
   
   public ManualScaleStrategy(VMChooser vmChooser, EDPolicy edPolicy) {
      _vmChooser = vmChooser;
      _enableDisablePolicy = edPolicy;
   }

   class CallableStrategy implements Callable {
      final ClusterScaleEvent _event;
      
      public CallableStrategy(ClusterScaleEvent event) {
         _event = event;
      }

      @Override
      public Object call() throws Exception {
         if (_event instanceof SerengetiLimitEvent) {
            SerengetiLimitEvent limitEvent = (SerengetiLimitEvent)_event;
            ClusterMap clusterMap = getAndReadLockClusterMap();
            String clusterId = clusterMap.getClusterIdForFolder(limitEvent.getClusterFolderName());
            int poweredOnVms = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, true).size();
            int delta = limitEvent.getToSize() - poweredOnVms;
            Set<String> vmsToED;
            if (delta > 0) {
               vmsToED = _vmChooser.chooseVMsToEnable(clusterId, clusterMap, delta);
               if (vmsToED != null) {
                  _enableDisablePolicy.enableTTs(vmsToED, limitEvent.getToSize(), clusterId, clusterMap);
               }
            } else if (delta < 0) {
               vmsToED = _vmChooser.chooseVMsToDisable(clusterId, clusterMap, delta);
               if (vmsToED != null) {
                  _enableDisablePolicy.disableTTs(vmsToED, limitEvent.getToSize(), clusterId, clusterMap);
               }
            }
         }
         return null;
      }
      
   }

   @Override
   public Callable getCallable(ClusterScaleEvent event) {
      return new CallableStrategy(event);
   }
}
