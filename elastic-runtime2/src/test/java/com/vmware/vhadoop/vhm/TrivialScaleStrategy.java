package com.vmware.vhadoop.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;

public class TrivialScaleStrategy implements ScaleStrategy {
   String _testKey;
   
   public TrivialScaleStrategy(String testKey) {
      _testKey = testKey;
   }

   @Override
   public void initialize(ClusterMapReader parent) {
   }

   @Override
   public ClusterMap getAndReadLockClusterMap() {
      return null;
   }

   @Override
   public void unlockClusterMap(ClusterMap clusterMap) {
   }

   @Override
   public String getKey() {
      return _testKey;
   }

   @Override
   public Class<? extends ScaleStrategyContext> getStrategyContextType() {
      return null;
   }

   @Override
   public ClusterScaleOperation getClusterScaleOperation(String clusterId,
         Set<ClusterScaleEvent> events, ScaleStrategyContext context) {
      return null;
   }

}
