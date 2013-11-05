package com.vmware.vhadoop.vhm.strategy;

import java.util.HashSet;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class PowerOnTimeVMChooser extends AbstractClusterMapReader implements VMChooser, ClusterMapReader {
   private final Long _thresholdMillis;
   
   /* If a threshold is passed in, the choose methods will only choose VMs that have been powered on for less than the threshold */
   public PowerOnTimeVMChooser(Long powerOnTimeThresholdMillis) {
      _thresholdMillis = powerOnTimeThresholdMillis;
   }
   
   public PowerOnTimeVMChooser() {
      _thresholdMillis = null;
   }

   @Override
   public Set<String> chooseVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      if (_thresholdMillis == null) {
         return null;
      }
      return candidateVmIds;
   }
   
   private abstract class PowerTimeExtractor<T> {
      Set<String> _candidateVmIds = null;

      PowerTimeExtractor(Set<String> candidateVmIds) {
         _candidateVmIds = candidateVmIds;
      }
      
      Set<T> getResults() {
         Set<T> result = new HashSet<T>();
         if (_candidateVmIds != null) {
            ClusterMap clusterMap = null;
            long currentTimeMillis = System.currentTimeMillis();
            try {
               clusterMap = getAndReadLockClusterMap();
               for (String vmId : _candidateVmIds) {
                  Long powerOnTime = clusterMap.getPowerOnTimeForVm(vmId);
                  if (powerOnTime != null) {
                     T testResult = testVM(vmId, (currentTimeMillis - powerOnTime));
                     if (testResult != null) {
                        result.add(testResult);
                     }
                  }
               }
            } finally {
               unlockClusterMap(clusterMap);
            }
         }
         return result;
      }
      
      abstract T testVM(String vmId, long elapsedMillis);
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      if (_thresholdMillis == null) {
         return null;
      }
      PowerTimeExtractor<String> extractor = new PowerTimeExtractor<String>(candidateVmIds) {
         @Override
         String testVM(String vmId, long elapsedMillis) {
            return (elapsedMillis <= _thresholdMillis) ? vmId : null;
         }
      };
      return extractor.getResults();
   }

   @Override
   /* For enabling, this VMChooser ranks evenly as powerOnTime makes no sense for powered-off VMs */
   public Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      Set<RankedVM> result = new HashSet<RankedVM>();
      if (candidateVmIds != null) {
         for (String vmId : candidateVmIds) {
            result.add(new RankedVM(vmId, 0));
         }
      }
      return result;
   }

   @Override
   public Set<RankedVM> rankVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      PowerTimeExtractor<RankedVM> extractor = new PowerTimeExtractor<RankedVM>(candidateVmIds) {
         @Override
         RankedVM testVM(String vmId, long elapsedMillis) {
            return new RankedVM(vmId, (int)elapsedMillis);
         }
      };
      return RankedVM.flattenRankValues(extractor.getResults());
   }

}
