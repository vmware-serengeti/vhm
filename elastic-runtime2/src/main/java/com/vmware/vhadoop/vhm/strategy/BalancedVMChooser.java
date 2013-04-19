package com.vmware.vhadoop.vhm.strategy;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class BalancedVMChooser extends AbstractClusterMapReader implements VMChooser {
   private static final Logger _log = Logger.getLogger(BalancedVMChooser.class.getName());

   public Set<String> chooseVMs(final String clusterId, final int delta, final boolean targetPowerState) {
      class Host {
         Set<String> candidates;
         int on;
      }

      int remaining = delta;
      Set<String> result = new HashSet<String>();

      ClusterMap clusterMap = getAndReadLockClusterMap();
      Set<String> hosts = clusterMap.listHostsWithComputeVMsForCluster(clusterId);
      PriorityQueue<Host> targets = new PriorityQueue<Host>(hosts.size(), new Comparator<Host>() {
         @Override
         public int compare(final Host a, final Host b) { return targetPowerState ? a.on - b.on : b.on - a.on; }
      });

      /* build the entries for the priority queue */
      for (String host : hosts) {
         Set<String> on = clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, host, true);
         Set<String> candidateVMs = targetPowerState ? clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, host, false) : on;
         if (candidateVMs.size() > 0) {
            Host h = new Host();
            h.candidates = candidateVMs;
            h.on = on.size();
            targets.add(h);
         }
      }

      unlockClusterMap(clusterMap);

      while (remaining-- > 0) {
         Host host = targets.poll();
         if (host == null) {
            /* there are no candidates left, so return what we have */
            return result;
         }

         Iterator<String> itr = host.candidates.iterator();
         if (itr.hasNext()) {
            if (targetPowerState) {
               host.on++;
            } else {
               host.on--;
            }
            String vmid = itr.next();
            result.add(vmid);
            itr.remove();
            _log.info("BalancedVMChooser adding VM "+vmid+" to results");
         }

         /* if this host still has candidates remaining, then insert it back into the queue */
         if (itr.hasNext()) {
            targets.add(host);
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
      return chooseVMs(clusterId, delta, false);
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
