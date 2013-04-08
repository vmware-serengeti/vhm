package com.vmware.vhadoop.api.vhm.strategy;

import java.util.Set;
import java.util.concurrent.Callable;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;

public interface ScaleStrategy extends ClusterMapReader {

   String getName();
   
   Class<? extends ScaleStrategyContext> getStrategyContextType();
   
   abstract class ClusterScaleOperation implements Callable<ClusterScaleCompletionEvent> {
      private ClusterMapAccess _clusterMapAccess;
      
      /* Prevent ClusterMapOperations from sharing the ClusterMapAccess of their enclosing classes. Clone must be used */
      public ClusterScaleOperation(ClusterMapAccess clonedClusterMapAccess) {
         _clusterMapAccess = clonedClusterMapAccess;
      }
      
      public ClusterMap getAndReadLockClusterMap() {
         return _clusterMapAccess.lockClusterMap();
      }

      public void unlockClusterMap(ClusterMap clusterMap) {
         _clusterMapAccess.unlockClusterMap(clusterMap);
      }
   }
   
   ClusterScaleOperation getClusterScaleOperation(String clusterId, Set<ClusterScaleEvent> events, ScaleStrategyContext context);
}
