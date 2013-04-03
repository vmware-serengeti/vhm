package com.vmware.vhadoop.api.vhm.strategy;

import java.util.*;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public interface VMChooser extends ClusterMapReader {
   Set<String> chooseVMsToEnable(String clusterId, int delta);

   Set<String> chooseVMsToDisable(String clusterId, int delta);

   String chooseVMToEnableOnHost(Set<String> candidates);

   String chooseVMToDisableOnHost(Set<String> candidates);
}
