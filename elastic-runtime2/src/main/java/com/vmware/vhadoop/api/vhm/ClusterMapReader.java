package com.vmware.vhadoop.api.vhm;

/* Any class wanting to access ClusterMap data should implement ClusterMapReader
 * Easiest way to do this is to extend AbstractClusterMapReader.
 * ClusterMapReader provides a simple multiple-reader single-writer locking mechanism via the ClusterMapAccess interface
 * A ClusterMapReader can only be initialized with a reference to another ClusterMapReader - in this way, one reader can hand access to others
 */
public interface ClusterMapReader {
   
   public static final String POWER_STATE_CHANGE_STATUS_KEY = "blockOnPowerStateChange";

   public interface ClusterMapAccess {
      ClusterMap lockClusterMap();

      boolean unlockClusterMap(ClusterMap clusterMap);
   }
      
   void initialize(ClusterMapReader parent);
   
   /* Holding a read lock on ClusterMap ensures that it won't change until unlockClusterMap is called */
   ClusterMap getAndReadLockClusterMap();

   void unlockClusterMap(ClusterMap clusterMap);
}
