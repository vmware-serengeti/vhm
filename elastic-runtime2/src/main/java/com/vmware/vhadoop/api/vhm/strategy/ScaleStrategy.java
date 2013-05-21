package com.vmware.vhadoop.api.vhm.strategy;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public interface ScaleStrategy extends ClusterMapReader {

   String getKey();
   
   Class<? extends ScaleStrategyContext> getStrategyContextType();
   
   abstract class ClusterScaleOperation extends AbstractClusterMapReader implements Callable<ClusterScaleCompletionEvent> {
      private static final Logger _log = Logger.getLogger(ClusterScaleOperation.class.getName());

      @Override
      public ClusterScaleCompletionEvent call() {
         ClusterScaleCompletionEvent result = null;
         /* It is critical that this initialize call is matched by the remove() call below in order to avoid memory leaks */
         getThreadLocalCompoundStatus().initialize();
         try {
            result = localCall();
         } catch (Throwable t) {
            _log.log(Level.SEVERE, "Unexpected exception in ClusterScaleOperation", t);
         }
         getThreadLocalCompoundStatus().remove();
         return result;
      }
      
      public abstract ClusterScaleCompletionEvent localCall() throws Exception;
   }
   
   Class<? extends ClusterScaleEvent>[] getScaleEventTypesHandled();
   
   ClusterScaleOperation getClusterScaleOperation(String clusterId, Set<ClusterScaleEvent> events, ScaleStrategyContext context);
}
