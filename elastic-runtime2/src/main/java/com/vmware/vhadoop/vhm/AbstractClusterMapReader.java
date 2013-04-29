package com.vmware.vhadoop.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;

public abstract class AbstractClusterMapReader implements ClusterMapReader {

   private ClusterMapAccess _clusterMapAccess;
   private ThreadLocalCompoundStatus _threadLocalStatus;

   protected CompoundStatus getCompoundStatus() {
      if (_threadLocalStatus == null) {
         return new CompoundStatus("DUMMY_STATUS");
      }
      return _threadLocalStatus.get();
   }

   /* Any subclass wishing to add status to this thread's execution should call this method */
   public ThreadLocalCompoundStatus getThreadLocalCompoundStatus() {
      return _threadLocalStatus;
   }

   @Override
   public void registerClusterMapAccess(final ClusterMapAccess access, final ThreadLocalCompoundStatus threadLocalStatus) {
      _clusterMapAccess = access;
      _threadLocalStatus = threadLocalStatus;
   }

   @Override
   /* Gets a read lock on ClusterMap - call unlock when done */
   public ClusterMap getAndReadLockClusterMap() {
      return _clusterMapAccess.lockClusterMap();
   }

   @Override
   public void unlockClusterMap(final ClusterMap clusterMap) {
      _clusterMapAccess.unlockClusterMap(clusterMap);
   }

   @Override
   public ClusterMapAccess cloneClusterMapAccess() {
      return cloneClusterMapAccess(_clusterMapAccess);
   }

   public static ClusterMapAccess cloneClusterMapAccess(final ClusterMapAccess access) {
      if (access != null) {
         return access.clone();
      }

      return null;
   }

   @Override
   public void blockOnPowerStateChange(final Set<String> vmIds, final boolean expectedPowerState, final long timeout) {
      CompoundStatus status = new CompoundStatus(POWER_STATE_CHANGE_STATUS_KEY);
      long timeoutTime = System.currentTimeMillis() + timeout;
      long pollSleepTime = 500;
      boolean timedOut = false;
      do {
         try {
            ClusterMap clusterMap = _clusterMapAccess.lockClusterMap();
            boolean completed = clusterMap.checkPowerStateOfVms(vmIds, expectedPowerState);
            _clusterMapAccess.unlockClusterMap(clusterMap);
            if (completed) {
               status.registerTaskSucceeded();
               break;
            }
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
}
