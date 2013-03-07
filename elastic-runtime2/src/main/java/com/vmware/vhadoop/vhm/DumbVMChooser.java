package com.vmware.vhadoop.vhm;

import java.util.*;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.VMChooser;

public class DumbVMChooser implements VMChooser {

   public Set<String> chooseVMs(String clusterId, ClusterMap clusterMap, int delta, boolean powerState) {
      Set<String> vms = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, powerState);
      if (vms.size() <= delta) {
         return vms;
      }
      Set<String> result = new HashSet<String>();
      Iterator<String> iterator = vms.iterator();
      for (int i=0; i<delta; i++) {
         result.add(iterator.next());
      }
      return result;
   }
   
   @Override
   public Set<String> chooseVMsToEnable(String clusterId, ClusterMap clusterMap, int delta) {
      return chooseVMs(clusterId, clusterMap, delta, false);
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, ClusterMap clusterMap, int delta) {
      return chooseVMs(clusterId, clusterMap, 0-delta, true);
   }

}
