package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public abstract class AbstractJUnitTest {
   
   protected ClusterMap getTestClusterMap(boolean prepopulate) {
      return new StandaloneSimpleClusterMap(prepopulate);
   }
   
   protected ClusterMapReader getTestClusterMapReader(ClusterMap clusterMap) {
      MultipleReaderSingleWriterClusterMapAccess.destroy();
      MultipleReaderSingleWriterClusterMapAccess cma = 
            MultipleReaderSingleWriterClusterMapAccess.getClusterMapAccess(clusterMap);
      return new AbstractClusterMapReader(cma, null) {};
   }
   
}
