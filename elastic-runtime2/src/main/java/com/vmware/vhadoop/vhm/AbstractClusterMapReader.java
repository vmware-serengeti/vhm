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

package com.vmware.vhadoop.vhm;

import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;

public abstract class AbstractClusterMapReader implements ClusterMapReader {

   private static final Logger _log = Logger.getLogger(AbstractClusterMapReader.class.getName());

   ClusterMapAccess _clusterMapAccess;
   private ThreadLocalCompoundStatus _threadLocalStatus;
   private boolean _initialized;

   /* To be used purely to allow the parent to initialize these values */
   protected AbstractClusterMapReader(ClusterMapAccess clusterMapAccess, ThreadLocalCompoundStatus tlcs) {
      _clusterMapAccess = clusterMapAccess;
      _threadLocalStatus = tlcs;
      _initialized = true;
   }

   public AbstractClusterMapReader() {
      _initialized = false;
   }

   protected CompoundStatus getCompoundStatus() {
      if (_threadLocalStatus == null) {
         return new CompoundStatus("DUMMY_STATUS");
      }
      return _threadLocalStatus.get();
   }

   /* Any subclass wishing to add status to this thread's execution should call this method */
   protected ThreadLocalCompoundStatus getThreadLocalCompoundStatus() {
      return _threadLocalStatus;
   }

   @Override
   public void initialize(final ClusterMapReader parent) {
      if (parent instanceof AbstractClusterMapReader) {
         _clusterMapAccess = ((AbstractClusterMapReader)parent)._clusterMapAccess;
         _threadLocalStatus = ((AbstractClusterMapReader)parent)._threadLocalStatus;
         _initialized = true;
      } else {
         _log.severe("Unrecognized ClusterMapReader implementation");
      }
   }

   private boolean checkInitialized() {
      if (!_initialized) {
         _log.severe("Method invocation failed due to uninitialized ClusterMapReader");
      }
      return _initialized;
   }

   @Override
   /* Gets a read lock on ClusterMap - call unlock when done */
   public ClusterMap getAndReadLockClusterMap() {
      ClusterMap result = null;
      if (checkInitialized()) {
         result = _clusterMapAccess.lockClusterMap();
      }
      return (result != null) ? result : new NullClusterMap();
   }

   @Override
   public void unlockClusterMap(final ClusterMap clusterMap) {
      if (clusterMap instanceof NullClusterMap) {
         return;
      }
      _clusterMapAccess.unlockClusterMap(clusterMap);
   }

   public void blockOnPowerStateChange(final Set<String> vmIds, final boolean expectedPowerState, final long timeout) {
      CompoundStatus status = new CompoundStatus(POWER_STATE_CHANGE_STATUS_KEY);
      long timeoutTime = System.currentTimeMillis() + timeout;
      long pollSleepTime = 500;
      boolean timedOut = false;
      ClusterMap clusterMap = null;

      if (vmIds == null || vmIds.isEmpty()) {
         return;
      }

      do {
         boolean completed = true;
         
         try {
            clusterMap = getAndReadLockClusterMap();     /* Initialization check here */
            
            for (String vmId : vmIds) {
               Boolean result = clusterMap.checkPowerStateOfVm(vmId, expectedPowerState);
               if (result == null) {
                  _log.fine("checkPowerState cannot find VM <%V"+vmId);
               } else if (!result) {
                  completed = false;
                  break;
               }
            }
         } finally {
            unlockClusterMap(clusterMap);
         }

         if (completed) {
            status.registerTaskSucceeded();
            break;
         }

         try {
            Thread.sleep(Math.min(pollSleepTime, timeout));
         } catch (InterruptedException e) {
            status.registerTaskIncomplete(false, "blockOnPowerStateChange was interrupted unexpectedly");
         }
         timedOut = System.currentTimeMillis() > timeoutTime;
      } while (!timedOut);
      if (timedOut) {
         status.registerTaskFailed(false, "Timeout waiting for powerStateChange");
      }
      getCompoundStatus().addStatus(status);
   }
   
   public String getMasterVmIdForCluster(String clusterId) {
      ClusterMap clusterMap = null;
      try {
         clusterMap = getAndReadLockClusterMap();     /* Initialization check here */
         return clusterMap.getMasterVmIdForCluster(clusterId);
      } finally {
         unlockClusterMap(clusterMap);
      }
   }
}
