package com.vmware.vhadoop.vhm.strategy;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class BalancedVMChooser extends AbstractClusterMapReader implements VMChooser {
   private static final Logger _log = Logger.getLogger(BalancedVMChooser.class.getName());

   private void addAndRemove(final Set<String> result, final Iterator<String> itr) {
      String vmid = itr.next();
      result.add(vmid);
      _log.info("BalancedVMChooser adding VM "+vmid+" to results");
      itr.remove();
   }

   public Set<String> chooseVMs(final String clusterId, final int delta, final boolean targetPowerState) {
      int remaining = Math.abs(delta);

      Set<String> result = new HashSet<String>();
      ClusterMap clusterMap = getAndReadLockClusterMap();
      Set<String> hosts = clusterMap.listHostsWithComputeVMsForCluster(clusterId);
      Set<String> computeVMs = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, true);

      /* round up when powering down, round down when powering up */
      int rounding = targetPowerState ? 0 : hosts.size() - 1;
      /* number of VMs to have powered on per host if possible */
      int targetPerHost = (computeVMs.size() + delta + rounding ) / hosts.size();

      /* build a list of VMs, ordered in priority order for the action we want */
      LinkedList<Set<String>> remainderSet = new LinkedList<Set<String>>();
      for (String host : hosts) {
         Set<String> on = clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, host, true);
         Set<String> off = clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, host, false);
         Set<String> candidateVMs = targetPowerState ? off : on;
         int candidates = candidateVMs.size();

         int deltaV = targetPerHost - on.size();

         /* if there are any immediate candidates for change in this host do it now */
         Iterator<String> itr = candidateVMs.iterator();
         for (int i = 0; remaining > 0 && i < candidates && i < deltaV; i++) {
            addAndRemove(result, itr);
            remaining--;
         }

         /* candidates left for remainder */
         if (candidateVMs.size() > 0) {
            remainderSet.add(candidateVMs);
         }
      }

      unlockClusterMap(clusterMap);

      /* if we don't have any more candidate VMs return what we can */
      if (remainderSet.size() == 0) {
         return result;
      }

      /* at this point all hosts with candidates remaining should be at the target per host value. Round robin until we're done */
      while (remaining > 0 && !remainderSet.isEmpty()) {
         Iterator<Set<String>> itr = remainderSet.iterator();
         while (itr.hasNext() && remaining > 0) {
            Set<String> vmsOnHost = itr.next();
            Iterator<String> itr2 = vmsOnHost.iterator();
            addAndRemove(result, itr2);
            remaining--;
            if (vmsOnHost.isEmpty()) {
               itr.remove();
            }
         }
      }

      return result;
   }

   @Override
   public Set<String> chooseVMsToEnable(final String clusterId, final int delta) {
      return chooseVMs(clusterId, delta, true);
   }

   /**
    * Delta is negative for disabling VMs
    */
   @Override
   public Set<String> chooseVMsToDisable(final String clusterId, final int delta) {
      return chooseVMs(clusterId, 0 - delta, false);
   }

   @Override
   public String chooseVMToEnableOnHost(final Set<String> candidates) {
      /* Not implemented */
      return null;
   }

   @Override
   public String chooseVMToDisableOnHost(final Set<String> candidates) {
      /* Not implemented */
      return null;
   }

}
