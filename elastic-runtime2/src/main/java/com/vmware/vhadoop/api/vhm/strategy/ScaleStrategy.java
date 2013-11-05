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

public interface ScaleStrategy extends ClusterMapReader {

   interface VMChooserCallback {
      Set<VMChooser> getVMChoosers();
      
      VMChooser getVMChooserForType(Class<? extends VMChooser> vmChooser);
   }

   String getKey();
   
   @Override
   void initialize(ClusterMapReader parent);
   
   void setVMChooserCallback(VMChooserCallback callback);
   
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
            _log.log(Level.SEVERE, "VHM: unexpected exception while scaling", t);
         }
         getThreadLocalCompoundStatus().remove();
         return result;
      }

      public abstract ClusterScaleCompletionEvent localCall() throws Exception;
   }

   Class<? extends ClusterScaleEvent>[] getScaleEventTypesHandled();

   ClusterScaleOperation getClusterScaleOperation(String clusterId, Set<ClusterScaleEvent> events, ScaleStrategyContext context);
}
