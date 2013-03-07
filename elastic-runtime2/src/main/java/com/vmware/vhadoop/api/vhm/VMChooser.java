package com.vmware.vhadoop.api.vhm;

import java.util.*;

import com.vmware.vhadoop.api.vhm.ClusterMap;

public interface VMChooser {
   Set<String> chooseVMsToEnable(String clusterId, ClusterMap clusterMap, int delta);

   Set<String> chooseVMsToDisable(String clusterId, ClusterMap clusterMap, int delta);
}
