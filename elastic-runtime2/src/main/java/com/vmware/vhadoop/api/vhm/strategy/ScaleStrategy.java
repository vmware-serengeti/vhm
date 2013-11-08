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

package com.vmware.vhadoop.api.vhm.strategy;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

/**
 * Component that represents a strategy for the scaling up or down of a Hadoop Cluster
 * A @ScaleStrategy implementation is registered as a singleton and each time a cluster needs to be scaled
 *   it is asked for an instance of a @ClusterScaleOperation which will be invoked as a @Callable in its own thread
 * A @ScaleStrategy is also a @ClusterMapReader so that it can access data in @ClusterMap
 */
public interface ScaleStrategy extends ClusterMapReader {

   /**
    * In order to scale a cluster up or down, a @ScaleStrategy needs to choose Task Tracker VMs to enable or disable
    * It can use any number of @VMChooser objects to help it to do this. It can access VMChoosers registered with VHM
    *   via this callback interface
    */
   interface VMChooserCallback {
      /**
       * Return all @VMChooser objects registered with VHM
       * Each @VMChooser will be able to assist in providing context when choosing VMs to enable or disable
       * @return A set of all VMChoosers registered with VHM
       */
      Set<VMChooser> getVMChoosers();
      
      /**
       * Return a @VMChooser that matches a specific type
       * @param vmChooser A class representing a type of VMChooser to return
       * @return The instance if found, null otherwise
       */
      VMChooser getVMChooserForType(Class<? extends VMChooser> vmChooser);
   }

   /**
    * Each ScaleStrategy has a unique key constant that is associated with each cluster
    * @return The key of this scale strategy
    */
   String getKey();
   
   /**
    * A @VMChooserCallback is passed in during initialization
    * @param callback
    */
   void setVMChooserCallback(final VMChooserCallback callback);

   /**
    * A @ScaleStrategyContext is a per-cluster object that the ScaleStrategy can use to store context about a cluster
    * An instance of @ScaleStrategyContext per-cluster is stored in the @ExectionStrategy and is passed in for each invocation
    * @return The class type of @ScaleStrategyContext that should be created for this @ScaleStrategy
    */
   Class<? extends ScaleStrategyContext> getStrategyContextType();

   /**
    * The @ScaleStrategy is invoked for a cluster when certain events arrive on the VHM event queue
    * This method allows the @ScaleStrategy to define which event types it should be invoked for
    * @return An array of event types for which the @ScaleStrategy should be invoked
    */
   Class<? extends ClusterScaleEvent>[] getScaleEventTypesHandled();

   /**
    * A @ClusterScaleOperation represents the per-cluster processing of a @ScaleStrategy.
    * It is @Callable as it is invoked in its own thread
    * @ScaleStrategy should return a new instance of @ClusterScaleOperation for each invocation of the strategy
    * As such, @ClusterScaleOperation is stateless. Persistent state can be stored in the @ScaleStrategyContext passed in
    * @ClusterScaleOperation is also a @ClusterMapReader to allow it access to cluster state
    * A @ClusterScaleOperation should return a @ClusterScaleCompletionEvent representing the result of the scale operation
    */
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
            _log.log(Level.SEVERE, "VHM: unexpected exception while scaling", t);
         }
         getThreadLocalCompoundStatus().remove();
         return result;
      }

      public abstract ClusterScaleCompletionEvent localCall() throws Exception;
   }

   /**
    * Returns a new instance of @ClusterScaleOperation which is invoked by the @ExecutionStrategy
    * 
    * @param clusterId The cluster id of the cluster to scale
    * @param events All the events for the cluster that have arrived since the last time the strategy was invoked
    * @param context The @ScaleStrategyContext instance for the cluster
    * @return A @ClusterScaleOperation representing the result of the scale operation
    */
   ClusterScaleOperation getClusterScaleOperation(String clusterId, Set<ClusterScaleEvent> events, ScaleStrategyContext context);
}
