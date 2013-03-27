package com.vmware.vhadoop.api.vhm;

public interface ClusterMapReader {

   public interface ClusterMapAccess {
      public ClusterMap lockClusterMap();

      public void unlockClusterMap(ClusterMap clusterMap);
   }
   
   void registerClusterMapAccess(ClusterMapAccess access);
   
   ClusterMap getAndReadLockClusterMap();

   void unlockClusterMap(ClusterMap clusterMap);
}
