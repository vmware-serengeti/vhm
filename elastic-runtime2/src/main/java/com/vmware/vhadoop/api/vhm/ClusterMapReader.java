package com.vmware.vhadoop.api.vhm;

public interface ClusterMapReader {

   public interface ClusterMapAccess {
      ClusterMap lockClusterMap();

      void unlockClusterMap(ClusterMap clusterMap);
      
      ClusterMapAccess clone();
   }
   
   void registerClusterMapAccess(ClusterMapAccess access);
   
   ClusterMap getAndReadLockClusterMap();

   void unlockClusterMap(ClusterMap clusterMap);

   ClusterMapAccess cloneClusterMapAccess();
}
