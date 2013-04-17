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
   
   public ThreadLocalCompoundStatus getThreadLocalCompoundStatus() {
      return _threadLocalStatus;
   }
   
   @Override
   public void registerClusterMapAccess(ClusterMapAccess access, ThreadLocalCompoundStatus threadLocalStatus) {
      _clusterMapAccess = access;
      _threadLocalStatus = threadLocalStatus;
   }

   @Override
   /* Gets a read lock on ClusterMap - call unlock when done */
   public ClusterMap getAndReadLockClusterMap() {
      return _clusterMapAccess.lockClusterMap();
   }
   
   @Override
   public void unlockClusterMap(ClusterMap clusterMap) {
      _clusterMapAccess.unlockClusterMap(clusterMap);
   }

   @Override
   public ClusterMapAccess cloneClusterMapAccess() {
      if (_clusterMapAccess != null) {
         return _clusterMapAccess.clone();
      }
      return null;
   }
   
   @Override
   public void blockOnPowerStateChange(Set<String> vmIds, boolean expectedPowerState, long timeout) {
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
