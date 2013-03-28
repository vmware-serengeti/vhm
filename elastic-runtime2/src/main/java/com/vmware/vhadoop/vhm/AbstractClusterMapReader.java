package com.vmware.vhadoop.vhm;

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
}
