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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class BalancedVMChooser extends AbstractClusterMapReader implements VMChooser {
   private static final Logger _log = Logger.getLogger(BalancedVMChooser.class.getName());

   private class Host {
      Set<String> candidates;
      int on;
   }

   /**
    * Selects the VMs to operate on from the candidates passed in. Candidates are passed in grouped via host and held in a priority queue.
    * The queues comparator uses the details held per host to determine which host should be the next to have a VM operated on.
    *
    * Currently we operate on candidates on the host in the order they're found, however we could pair this with a call to chooseVmOnHost
    * to provide a more discriminating mechanism.
    *
    * @param targets - candidate VMs, grouped by host
    * @param delta - the number of VMs to operate on (>= 0)
    * @param targetPowerState - true to enable VMs, false to disable
    * @return the chosen set of VMs
    */
   protected Set<String> selectVMs(final Queue<Host> targets, final int delta, final boolean targetPowerState) {
      Set<String> result = new HashSet<String>();
      int remaining = Math.abs(delta);

      while (remaining-- > 0) {
         Host host = targets.poll();
         if (host == null) {
            /* there are no candidates left, so return what we have */
            if (delta != Integer.MAX_VALUE) {
               _log.warning("VHM: no more hosts with candidate VMs, shortfall is "+(remaining+1));
            }
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
            _log.info("BalancedVMChooser adding VM <%V"+vmid+"%V> to results");
         }

         /* if this host still has candidates remaining, then insert it back into the queue */
         if (itr.hasNext()) {
            targets.add(host);
         }
      }

      return result;
   }

   public Set<String> chooseVMs(final String clusterId, final int delta, final boolean targetPowerState) {
      Set<String> result = null;
      Set<String> hosts = null;
      int numHosts = 0;

      ClusterMap clusterMap = null;
      try {
         clusterMap = getAndReadLockClusterMap();
         hosts = clusterMap.listHostsWithComputeVMsForCluster(clusterId);
         if ((hosts == null) || (hosts.size() == 0)) {
            result = new TreeSet<String>();     /* Return empty set, but don't return here!! */
         } else {
            numHosts = hosts.size();
         }
      } finally {
         unlockClusterMap(clusterMap);
      }

      if (result == null) {
         PriorityQueue<Host> targets = new PriorityQueue<Host>(numHosts, new Comparator<Host>() {
            @Override
            public int compare(final Host a, final Host b) { return targetPowerState ? a.on - b.on : b.on - a.on; }
         });

         /* build the entries for the priority queue */
         for (String host : hosts) {
            Set<String> on = clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, host, true);
            Set<String> candidateVMs = targetPowerState ? clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, host, false) : on;

            if ((candidateVMs != null) && (candidateVMs.size() > 0)) {
               /* this is a temporary solution that prevents us from returning the same VM on a host every time */
               List<String> vms = new ArrayList<String>(candidateVMs);
               Collections.shuffle(vms);
               candidateVMs = new LinkedHashSet<String>(vms);

               _log.info("found "+candidateVMs.size()+" candidate VMs on host "+host);
               for (String id : candidateVMs) {
                  _log.info("candidate VM on "+host+": <%V"+id);
               }
               if (on != null) {
                  _log.info("found "+on.size()+" VMs powered on, on host "+host);
                  for (String id : on) {
                     _log.fine("powered on VM on "+host+": <%V"+id);
                  }
               }
               Host h = new Host();
               h.candidates = candidateVMs;
               h.on = (on == null) ? 0 : on.size();
               targets.add(h);
            }
         }
         result = selectVMs(targets, delta, targetPowerState);
      }

      return result;
   }

   public Set<RankedVM> rankVMs(final String clusterId, final boolean targetPowerState) {
      Set<RankedVM> orderedResult = new TreeSet<RankedVM>();
      Set<String> chosenVMs = chooseVMs(clusterId, Integer.MAX_VALUE, targetPowerState);
      if (chosenVMs == null) {
         return null;
      }
      int rank = 0;
      for (String vmId : chosenVMs) {
         orderedResult.add(new RankedVM(vmId, rank++));
      }
      return orderedResult;
   }

   @Override
   public Set<String> chooseVMsToEnable(final String clusterId, final int delta) {
      return chooseVMs(clusterId, delta, true);
   }

   @Override
   public Set<String> chooseVMsToDisable(final String clusterId, final int delta) {
      return chooseVMs(clusterId, delta, false);
   }

   @Override
   public Set<RankedVM> rankVMsToEnable(String clusterId) {
      return rankVMs(clusterId, true);
   }

   @Override
   public Set<RankedVM> rankVMsToDisable(String clusterId) {
      return rankVMs(clusterId, false);
   }
}
