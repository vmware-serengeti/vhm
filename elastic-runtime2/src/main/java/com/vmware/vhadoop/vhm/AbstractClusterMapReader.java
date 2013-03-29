package com.vmware.vhadoop.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public abstract class AbstractClusterMapReader implements ClusterMapReader {

   private ClusterMapAccess _clusterMapAccess;
   
   @Override
   public void registerClusterMapAccess(ClusterMapAccess access) {
      _clusterMapAccess = access;
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
      long timeoutTime = System.currentTimeMillis() + timeout;
      long pollSleepTime = 500;
      do {
         try {
            ClusterMap clusterMap = _clusterMapAccess.lockClusterMap();
            boolean completed = clusterMap.checkPowerStateOfVms(vmIds, expectedPowerState);
            _clusterMapAccess.unlockClusterMap(clusterMap);
            if (completed) {
               break;
            }
            Thread.sleep(Math.min(pollSleepTime, timeout));
         } catch (InterruptedException e) { }
      } while (System.currentTimeMillis() <= timeoutTime);
   }
}
