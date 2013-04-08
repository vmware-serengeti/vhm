package com.vmware.vhadoop.api.vhm;

import java.util.Set;

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
   
   void blockOnPowerStateChange(Set<String> vmIds, boolean expectedPowerState, long timeout);
}
