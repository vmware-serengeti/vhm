package com.vmware.vhadoop.vhm.strategy;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class JobTrackerEDPolicy extends AbstractClusterMapReader implements EDPolicy {
   final HadoopActions _hadoopActions;
   final VCActions _vcActions;

   public JobTrackerEDPolicy(HadoopActions hadoopActions, VCActions vcActions) {
      _hadoopActions = hadoopActions;
      _vcActions = vcActions;
   }

   @Override
   public Set<String> enableTTs(Set<String> toEnable, int totalTargetEnabled, String clusterId) throws Exception {
      Map<String, String> hostNames = null;
      HadoopClusterInfo hadoopCluster = null;
      Set<String> activeVmIds = null;

      ClusterMap clusterMap = getAndReadLockClusterMap();
      try {
         hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
         hostNames = clusterMap.getDnsNameForVMs(toEnable);
      } finally {
         unlockClusterMap(clusterMap);
      }

      if (hostNames != null) {
         CompoundStatus status = getCompoundStatus();
         /* TODO: Legacy code returns a CompoundStatus rather than modifying thread local version. Ideally it would be refactored for consistency */
         _hadoopActions.recommissionTTs(hostNames, hadoopCluster);
         if (_vcActions.changeVMPowerState(toEnable, true) == null) {
            status.registerTaskFailed(false, "Failed to change VM power state in VC");
         } else {
            if (status.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_ON_STATUS_KEY})) {
               activeVmIds = _hadoopActions.checkTargetTTsSuccess("Recommission", hostNames, totalTargetEnabled, hadoopCluster);
            }
         }
      }
      return activeVmIds;
   }

   @Override
   public Set<String> disableTTs(Set<String> toDisable, int totalTargetEnabled, String clusterId) throws Exception {
      Map<String, String> hostNames = null;
      HadoopClusterInfo hadoopCluster = null;
      Set<String> activeVmIds = null;

      ClusterMap clusterMap = getAndReadLockClusterMap();
      try {
         hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
         hostNames = clusterMap.getDnsNameForVMs(toDisable);
      } finally {
         unlockClusterMap(clusterMap);
      }

      if (hostNames != null) {
         CompoundStatus status = getCompoundStatus();
         /* TODO: Legacy code returns a CompoundStatus rather than modifying thread local version. Ideally it would be refactored for consistency */
         _hadoopActions.decommissionTTs(hostNames, hadoopCluster);
         if (status.screenStatusesForSpecificFailures(new String[]{"decomRecomTTs"})) {
            activeVmIds = _hadoopActions.checkTargetTTsSuccess("Decommission", hostNames, totalTargetEnabled, hadoopCluster);
         }
         if (_vcActions.changeVMPowerState(toDisable, false) == null) {
            status.registerTaskFailed(false, "Failed to change VM power state in VC");
         }
      }
      return getSuccessfullyDisabledVmIds(toDisable, activeVmIds);
   }

   private Set<String> getSuccessfullyDisabledVmIds(Set<String> toDisable, Set<String> activeVmIds) {
      Set<String> result = new HashSet<String>();
      /* JG: If hostnames are screwed up (e.g., become localhost, etc.), activeVmIds can be null */
      if (activeVmIds == null) { return result; }
      for (String testDisabled : toDisable) {
         if (!activeVmIds.contains(testDisabled)) {
            result.add(testDisabled);
         }
      }
      return result;
   }

   @Override
   public Set<String> getActiveTTs(String clusterId) throws Exception {
      HadoopClusterInfo hadoopCluster = null;

      ClusterMap clusterMap = getAndReadLockClusterMap();
      try {
         hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
      } finally {
         unlockClusterMap(clusterMap);
      }

      if (hadoopCluster != null) {
         return _hadoopActions.getActiveTTs(hadoopCluster, 0);
      }
      return null;
   }

}
