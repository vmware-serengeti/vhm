package com.vmware.vhadoop.api.vhm;

import java.util.Set;

public interface ClusterMap {

   Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState);

   String getClusterIdForFolder(String clusterFolderName);

}
