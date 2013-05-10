package com.vmware.vhadoop.vhm.strategy;

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
   public void enableTTs(Set<String> toEnable, int totalTargetEnabled, String clusterId) throws Exception {
      ClusterMap clusterMap = getAndReadLockClusterMap();
      HadoopClusterInfo hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
      Set<String> hostNames = clusterMap.getDnsNameForVMs(toEnable);
      unlockClusterMap(clusterMap);

      if (hostNames != null) {
         CompoundStatus status = getCompoundStatus();
         /* TODO: Legacy code returns a CompoundStatus rather than modifying thread local version. Ideally it would be refactored for consistency */
         String[] hostNameArray = hostNames.toArray(new String[0]);
         status.addStatus(_hadoopActions.recommissionTTs(hostNameArray, hadoopCluster));
         _vcActions.changeVMPowerState(toEnable, true);
         if (status.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_ON_STATUS_KEY})) {
            status.addStatus(_hadoopActions.checkTargetTTsSuccess("Recommission", hostNameArray, totalTargetEnabled, hadoopCluster));
         }
      }
   }

   @Override
   public void disableTTs(Set<String> toDisable, int totalTargetEnabled, String clusterId) throws Exception {
      ClusterMap clusterMap = getAndReadLockClusterMap();
      HadoopClusterInfo hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
      Set<String> hostNames = clusterMap.getDnsNameForVMs(toDisable);
      unlockClusterMap(clusterMap);

      if (hostNames != null) {
         CompoundStatus status = getCompoundStatus();
         /* TODO: Legacy code returns a CompoundStatus rather than modifying thread local version. Ideally it would be refactored for consistency */
         String[] hostNameArray = hostNames.toArray(new String[0]);
         status.addStatus(_hadoopActions.decommissionTTs(hostNameArray, hadoopCluster));
         if (status.screenStatusesForSpecificFailures(new String[]{"decomRecomTTs"})) {
            status.addStatus(_hadoopActions.checkTargetTTsSuccess("Decommission", hostNameArray, totalTargetEnabled, hadoopCluster));
         }
         _vcActions.changeVMPowerState(toDisable, false);
      }
   }

}
