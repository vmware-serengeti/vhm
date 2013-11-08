/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/
package com.vmware.vhadoop.vhm.strategy;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class PowerOnTimeVMChooser extends AbstractClusterMapReader implements VMChooser, ClusterMapReader {
   private static final Logger _log = Logger.getLogger(PowerOnTimeVMChooser.class.getName());

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
            _log.fine("Choosing VM <%V"+vmId+"%V> with elapsedMillis="+elapsedMillis);
            return (elapsedMillis <= _thresholdMillis) ? vmId : null;
         }
      };
      Set<String> result = extractor.getResults();
      _log.info("PowerOnTimeVMChooser done choosing VMs for disabling: "+result);
      return result;
   }

   @Override
   /* For enabling, this VMChooser ranks evenly as powerOnTime makes no sense for powered-off VMs */
   public Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      Set<RankedVM> result = new HashSet<RankedVM>();
      _log.info("PowerOnTimeVMChooser adding all candidate VMs to results with same rank");
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
            _log.fine("Ranking VM <%V"+vmId+"%V> with elapsedMillis="+elapsedMillis);
            return new RankedVM(vmId, (int)elapsedMillis);
         }
      };
      Set<RankedVM> result = RankedVM.flattenRankValues(extractor.getResults());
      _log.info("PowerOnTimeVMChooser done ranking VMs for disabling: "+result);
      return result;
   }

}
