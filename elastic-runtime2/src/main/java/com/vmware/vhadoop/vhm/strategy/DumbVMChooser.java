package com.vmware.vhadoop.vhm.strategy;

import java.util.*;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class DumbVMChooser extends AbstractClusterMapReader implements VMChooser {
   private static final Logger _log = Logger.getLogger(DumbVMChooser.class.getName());

   public Set<String> chooseVMs(String clusterId, int delta, boolean powerState) {
      _log.info("DumbVMChooser choosing VMs for cluster "+clusterId+" where delta="+delta+", powerState="+powerState);
      ClusterMap clusterMap = getAndReadLockClusterMap();
      Set<String> vms = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, powerState);
      unlockClusterMap(clusterMap);
      if (vms.size() <= delta) {
         return vms;
      }
      Set<String> result = new HashSet<String>();
      Iterator<String> iterator = vms.iterator();
      for (int i=0; i<delta; i++) {
         String vm = iterator.next();
         _log.info("DumbVMChooser adding VM "+vm+" to results");
         result.add(vm);
      }
      return result;
   }
   
   @Override
   public Set<String> chooseVMsToEnable(String clusterId, int delta) {
      return chooseVMs(clusterId, delta, false);
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, int delta) {
      return chooseVMs(clusterId, 0-delta, true);
   }

}
