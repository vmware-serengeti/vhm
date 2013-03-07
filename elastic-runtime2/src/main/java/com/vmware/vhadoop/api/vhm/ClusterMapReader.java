package com.vmware.vhadoop.api.vhm;

public interface ClusterMapReader {

   interface ClusterMapAccess {
      public ClusterMap accessClusterMap();
   }
   
   void registerClusterMapAccess(ClusterMapAccess access);
   
   ClusterMap getReadOnlyClusterMap();
}
