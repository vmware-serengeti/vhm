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
            int delta = limitEvent.getToSize() - limitEvent.getFromSize();
            ClusterMap clusterMap = getReadOnlyClusterMap();
            Set<String> vmsToED;
            if (delta > 0) {
               vmsToED = _vmChooser.chooseVMsToEnable(limitEvent.getClusterId(), clusterMap, delta);
               if (vmsToED != null) {
                  _enableDisablePolicy.enableTTs(vmsToED, limitEvent.getToSize(), limitEvent.getClusterId(), clusterMap);
               }
            } else if (delta < 0) {
               vmsToED = _vmChooser.chooseVMsToDisable(limitEvent.getClusterId(), clusterMap, delta);
               if (vmsToED != null) {
                  _enableDisablePolicy.disableTTs(vmsToED, limitEvent.getToSize(), limitEvent.getClusterId(), clusterMap);
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
