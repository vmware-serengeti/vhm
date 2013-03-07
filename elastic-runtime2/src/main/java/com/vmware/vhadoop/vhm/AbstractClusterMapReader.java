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
   public ClusterMap getReadOnlyClusterMap() {
      return _clusterMapAccess.accessClusterMap();
   }

}
