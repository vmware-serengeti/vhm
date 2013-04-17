package com.vmware.vhadoop.api.vhm.strategy;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;

public interface ScaleStrategy extends ClusterMapReader {

   String getName();
   
   Class<? extends ScaleStrategyContext> getStrategyContextType();
   
   abstract class ClusterScaleOperation implements Callable<ClusterScaleCompletionEvent> {
      private ClusterMapAccess _clusterMapAccess;
      private ThreadLocalCompoundStatus _threadLocalStatus;
      
      private static final Logger _log = Logger.getLogger(ClusterScaleOperation.class.getName());

      /* Prevent ClusterMapOperations from sharing the ClusterMapAccess of their enclosing classes. Clone must be used */
      public ClusterScaleOperation(ClusterMapAccess clonedClusterMapAccess, ThreadLocalCompoundStatus tlcs) {
         _clusterMapAccess = clonedClusterMapAccess;
         _threadLocalStatus = tlcs;
      }
      
      public ClusterMap getAndReadLockClusterMap() {
         return _clusterMapAccess.lockClusterMap();
      }

      public void unlockClusterMap(ClusterMap clusterMap) {
         _clusterMapAccess.unlockClusterMap(clusterMap);
      }
      
      @Override
      public ClusterScaleCompletionEvent call() {
         ClusterScaleCompletionEvent result = null;
         /* It is critical that this initialize call is matched by the remove() call below in order to avoid memory leaks */
         _threadLocalStatus.initialize();
         try {
            result = localCall();
         } catch (Throwable t) {
            _log.log(Level.SEVERE, "Unexpected exception in ClusterScaleOperation", t);
         }
         _threadLocalStatus.remove();
         return result;
      }
      
      CompoundStatus getCompoundStatus() {
         return _threadLocalStatus.get();
      }
      
      public abstract ClusterScaleCompletionEvent localCall() throws Exception;
   }
   
   ClusterScaleOperation getClusterScaleOperation(String clusterId, Set<ClusterScaleEvent> events, ScaleStrategyContext context);
}
