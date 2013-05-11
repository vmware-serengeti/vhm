package com.vmware.vhadoop.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;

public abstract class AbstractClusterMapReader implements ClusterMapReader {

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
         throw new RuntimeException("Unrecognized ClusterMapReader implementation");
      }
   }

   private void checkInitialized() {
      if (!_initialized) {
         throw new RuntimeException("ClusterMapReader not initialized!");
      }
   }
   
   @Override
   /* Gets a read lock on ClusterMap - call unlock when done */
   public ClusterMap getAndReadLockClusterMap() {
      checkInitialized();
      return _clusterMapAccess.lockClusterMap();
   }

   @Override
   public void unlockClusterMap(final ClusterMap clusterMap) {
      checkInitialized();
      _clusterMapAccess.unlockClusterMap(clusterMap);
   }

   public void blockOnPowerStateChange(final Set<String> vmIds, final boolean expectedPowerState, final long timeout) {
      checkInitialized();
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
