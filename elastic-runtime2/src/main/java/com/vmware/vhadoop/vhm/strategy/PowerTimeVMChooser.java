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
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class PowerTimeVMChooser extends AbstractClusterMapReader implements VMChooser, ClusterMapReader {
   private static final Logger _log = Logger.getLogger(PowerTimeVMChooser.class.getName());

   private final Long _powerOnTimeThresholdMillis;
   private final Long _powerOffTimeThresholdMillis;
   
   /* If a threshold is passed in, the choose methods will only choose VMs that have been powered on for less than the threshold
      The rank methods will rank anything that exceeds the threshold as the same rank */
   public PowerTimeVMChooser(Long powerOnTimeThresholdMillis, Long powerOffTimeThresholdMillis) {
      _powerOnTimeThresholdMillis = powerOnTimeThresholdMillis;
      _powerOffTimeThresholdMillis = powerOffTimeThresholdMillis;
   }
   
   public PowerTimeVMChooser() {
      _powerOnTimeThresholdMillis = null;
      _powerOffTimeThresholdMillis = null;
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
                  Long powerOffTime = clusterMap.getPowerOffTimeForVm(vmId);
                  if ((powerOnTime != null) || (powerOffTime != null)) {
                     T testResult = testVM(vmId, currentTimeMillis, powerOnTime, powerOffTime);
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
      
      /* Either powerOnTime or powerOffTime could be null */
      abstract T testVM(String vmId, long currentTimeMillis, Long powerOnTime, Long powerOffTime);
   }

   @Override
   public Set<String> chooseVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      if (_powerOffTimeThresholdMillis == null) {
         return null;
      }
      PowerTimeExtractor<String> extractor = new PowerTimeExtractor<String>(candidateVmIds) {
         @Override
         String testVM(String vmId, long currentTimeMillis, Long powerOnTime, Long powerOffTime) {
            if (powerOffTime == null) {
               return vmId;            /* If we have no power-off time info, it will likely have been powered off for a long time */
            }
            /* Choose VMs that have not recently been powered off */
            long elapsedMillis = currentTimeMillis - powerOffTime;
            _log.fine("Choosing VM <%V"+vmId+"%V> with elapsedMillis="+elapsedMillis);
            return (elapsedMillis > _powerOffTimeThresholdMillis) ? vmId : null;
         }
      };
      Set<String> result = extractor.getResults();
      _log.info("PowerOnTimeVMChooser done choosing VMs for enabling: "+LogFormatter.constructListOfLoggableVms(result));
      return result;
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      if (_powerOnTimeThresholdMillis == null) {
         return null;
      }
      PowerTimeExtractor<String> extractor = new PowerTimeExtractor<String>(candidateVmIds) {
         @Override
         String testVM(String vmId, long currentTimeMillis, Long powerOnTime, Long powerOffTime) {
            if (powerOnTime == null) {
               return null;
            }
            /* Choose VMs that have been recently powered on */
            long elapsedMillis = currentTimeMillis - powerOnTime;
            _log.fine("Choosing VM <%V"+vmId+"%V> with elapsedMillis="+elapsedMillis);
            return (elapsedMillis <= _powerOnTimeThresholdMillis) ? vmId : null;
         }
      };
      Set<String> result = extractor.getResults();
      _log.info("PowerOnTimeVMChooser done choosing VMs for disabling: "+LogFormatter.constructListOfLoggableVms(result));
      return result;
   }

   @Override
   public Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      PowerTimeExtractor<RankedVM> extractor = new PowerTimeExtractor<RankedVM>(candidateVmIds) {
         @Override
         RankedVM testVM(String vmId, long currentTimeMillis, Long powerOnTime, Long powerOffTime) {
            if (powerOffTime == null) {
               return new RankedVM(vmId, 0);            /* If we have no power-off time info, it will likely have been powered off for a long time */
            }
            /* VMs that are over the threshold should be ranked evenly at 0, vms under the threshold should have a rank inverse to the elapsedMillis */
            long elapsedMillis = currentTimeMillis - powerOffTime;
            _log.fine("Ranking VM <%V"+vmId+"%V> with elapsedMillis="+elapsedMillis);
            int rank = ((_powerOffTimeThresholdMillis != null) && (elapsedMillis > _powerOffTimeThresholdMillis)) ? 0 : Integer.MAX_VALUE - (int)elapsedMillis;
            return new RankedVM(vmId, rank);
         }
      };
      Set<RankedVM> result = RankedVM.flattenRankValues(extractor.getResults());
      _log.info("PowerOnTimeVMChooser done ranking VMs for enabling: "+result);
      return result;
   }

   @Override
   public Set<RankedVM> rankVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      PowerTimeExtractor<RankedVM> extractor = new PowerTimeExtractor<RankedVM>(candidateVmIds) {
         @Override
         RankedVM testVM(String vmId, long currentTimeMillis, Long powerOnTime, Long powerOffTime) {
            if (powerOnTime == null) {
               return new RankedVM(vmId, Integer.MAX_VALUE);
            }
            /* VMs that are over the threshold should be ranked evenly at MAX_VALUE, vms under the threshold should have a rank in the same order as elapsedMillis */
            long elapsedMillis = currentTimeMillis - powerOnTime;
            _log.fine("Ranking VM <%V"+vmId+"%V> with elapsedMillis="+elapsedMillis);
            int rank = ((_powerOnTimeThresholdMillis != null) && (elapsedMillis > _powerOnTimeThresholdMillis)) ? Integer.MAX_VALUE : (int)elapsedMillis;
            return new RankedVM(vmId, rank);
         }
      };
      Set<RankedVM> result = RankedVM.flattenRankValues(extractor.getResults());
      _log.info("PowerOnTimeVMChooser done ranking VMs for disabling: "+result);
      return result;
   }

}
