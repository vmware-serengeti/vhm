package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public abstract class AbstractClusterMapReader implements ClusterMapReader {

   public ClusterMapAccess _clusterMapAccess;
   
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

}
