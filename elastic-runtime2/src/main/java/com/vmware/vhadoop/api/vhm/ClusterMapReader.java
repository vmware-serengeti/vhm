package com.vmware.vhadoop.api.vhm;

import java.util.Set;

import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;

public interface ClusterMapReader {
   
   public static final String POWER_STATE_CHANGE_STATUS_KEY = "blockOnPowerStateChange";

   public interface ClusterMapAccess {
      ClusterMap lockClusterMap();

      void unlockClusterMap(ClusterMap clusterMap);
      
      ClusterMapAccess clone();
   }
      
   void registerClusterMapAccess(ClusterMapAccess access, ThreadLocalCompoundStatus threadLocalStatus);
   
   ClusterMap getAndReadLockClusterMap();

   void unlockClusterMap(ClusterMap clusterMap);

   ClusterMapAccess cloneClusterMapAccess();
   
   void blockOnPowerStateChange(Set<String> vmIds, boolean expectedPowerState, long timeout);
}
