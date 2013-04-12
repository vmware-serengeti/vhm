package com.vmware.vhadoop.vhm.strategy;

import java.util.*;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class BalancedVMChooser extends AbstractClusterMapReader implements VMChooser {
   private static final Logger _log = Logger.getLogger(BalancedVMChooser.class.getName());

   private Set<String> trackVMsInState(ClusterMap clusterMap,
         Map<String, Set<String>> tracker, String clusterId, String host,
         boolean powerState) {
      Set<String> result = tracker.get(host);
      if (result == null) {
         result = clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, host, powerState);
         tracker.put(host, result);
      }
      return result;
   }

   public Set<String> chooseVMs(String clusterId, int delta, boolean targetPowerState) {
      Set<String> result = new HashSet<String>();
      ClusterMap clusterMap = getAndReadLockClusterMap();
      Set<String> candidateVMs = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, !targetPowerState);
      Set<String> hosts = clusterMap.listHostsWithComputeVMsForCluster(clusterId);
      Map<String, Set<String>> candidateTracker = new HashMap<String, Set<String>>();
      Map<String, Set<String>> targetTracker = new HashMap<String, Set<String>>();

      for (int targetPerHost = 0; targetPerHost <= candidateVMs.size(); targetPerHost++) {
         int remaining = delta - result.size();
         if (remaining <= 0) {
             break;
         }
         for (String host : hosts) {
            Set<String> vmsInCandidateState = trackVMsInState(clusterMap, candidateTracker, clusterId, host, !targetPowerState);
            if (vmsInCandidateState.size() > 0) {
               Set<String> vmsInTargetState = trackVMsInState(clusterMap, targetTracker, clusterId, host, targetPowerState);
               if (vmsInTargetState.size() == targetPerHost) {
                  String vm = vmsInCandidateState.iterator().next();
                  result.add(vm);
                  vmsInTargetState.add(vm);
                  vmsInCandidateState.remove(vm);
                  _log.info("BalancedVMChooser adding VM "+vm+" to results");
                  if (--remaining <= 0) {
                     break;
                  }
               }
            }
         }
      }
      unlockClusterMap(clusterMap);
      return result;
   }

   @Override
   public Set<String> chooseVMsToEnable(String clusterId, int delta) {
      return chooseVMs(clusterId, delta, true);
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, int delta) {
      return chooseVMs(clusterId, 0-delta, false);
   }

   @Override
   public String chooseVMToEnableOnHost(Set<String> candidates) {
      /* Not implemented */
      return null;
   }

   @Override
   public String chooseVMToDisableOnHost(Set<String> candidates) {
      /* Not implemented */
      return null;
   }

}
