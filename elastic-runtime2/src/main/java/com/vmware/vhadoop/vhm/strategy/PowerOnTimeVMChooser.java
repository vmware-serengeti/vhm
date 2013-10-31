package com.vmware.vhadoop.vhm.strategy;

import java.util.HashSet;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class PowerOnTimeVMChooser extends AbstractClusterMapReader implements VMChooser, ClusterMapReader {

   @Override
   public Set<String> chooseVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      return null;
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      return null;
   }

   @Override
   /* For enabling, this VMChooser ranks evenly as powerOnTime makes no sense for powered-off VMs */
   public Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      Set<RankedVM> result = new HashSet<RankedVM>();
      for (String vmId : candidateVmIds) {
         result.add(new RankedVM(vmId, 0));
      }
      return result;
   }

   @Override
   public Set<RankedVM> rankVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      ClusterMap clusterMap = null;
      Set<RankedVM> absoluteRank = new HashSet<RankedVM>();
      long currentTimeMillis = System.currentTimeMillis();
      try {
         clusterMap = getAndReadLockClusterMap();
         for (String vmId : candidateVmIds) {
            Long powerOnTime = clusterMap.getPowerOnTimeForVm(vmId);
            if (powerOnTime != null) {
               long elapsedMillis = currentTimeMillis - powerOnTime;
               absoluteRank.add(new RankedVM(vmId, (int)elapsedMillis));
            }
         }
      } finally {
         unlockClusterMap(clusterMap);
      }
      return RankedVM.flattenRankValues(absoluteRank);
   }

}
