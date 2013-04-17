package com.vmware.vhadoop.api.vhm;

import java.util.Set;

import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;

/* Any class wanting to access ClusterMap data should implement ClusterMapReader
 * Easiest way to do this is to extend AbstractClusterMapReader.
 * ClusterMapReader provides a simple multiple-reader single-writer locking mechanism via the ClusterMapAccess interface
 * Each ClusterMapReader should have its own instance of ClusterMapAccess. If a ClusterMapReader wants to initialize another ClusterMapReader,
 *   it should clone its own ClusterMapAccess instance when it calls registerClusterMapAccess() on the new ClusterMapReader instance.
 */
public interface ClusterMapReader {
   
   public static final String POWER_STATE_CHANGE_STATUS_KEY = "blockOnPowerStateChange";

   public interface ClusterMapAccess {
      ClusterMap lockClusterMap();

      void unlockClusterMap(ClusterMap clusterMap);
      
      ClusterMapAccess clone();
   }
      
   void registerClusterMapAccess(ClusterMapAccess access, ThreadLocalCompoundStatus threadLocalStatus);
   
   /* Holding a read lock on ClusterMap ensures that it won't change until unlockClusterMap is called */
   ClusterMap getAndReadLockClusterMap();

   void unlockClusterMap(ClusterMap clusterMap);

   ClusterMapAccess cloneClusterMapAccess();
   
   void blockOnPowerStateChange(Set<String> vmIds, boolean expectedPowerState, long timeout);
}
