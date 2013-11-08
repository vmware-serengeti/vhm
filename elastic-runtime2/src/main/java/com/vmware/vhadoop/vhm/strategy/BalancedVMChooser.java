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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class BalancedVMChooser extends AbstractClusterMapReader implements VMChooser, ClusterMapReader {
   private static final Logger _log = Logger.getLogger(BalancedVMChooser.class.getName());

   private class HostInfo {
      public HostInfo(String hostId) {
         _hostId = hostId;
      }
      String _hostId;
      Set<String> _candidates = new LinkedHashSet<String>();
      int _on;
   }

   private Set<RankedVM> rankVMs(final Queue<HostInfo> orderedHosts, final boolean targetPowerState) {
      Set<RankedVM> result = new HashSet<RankedVM>();
      int rank = 0;
      HostInfo current = null;
      
      do {
         current = orderedHosts.poll();
         if (current != null) {
            Iterator<String> itr = current._candidates.iterator();
            if (itr.hasNext()) {
               String vmId = itr.next();
               current._on += targetPowerState ? 1 : -1;
               result.add(new RankedVM(vmId, rank++));
               itr.remove();
               _log.info("BalancedVMChooser adding VM <%V"+vmId+"%V> from host "+current._hostId+" to results");
            }
            if (itr.hasNext()) {
               orderedHosts.add(current);
            }
         }
      } while (current != null);
      
      return result;
   }

   private Map<String, HostInfo> organizeVMsByHost(final String clusterId, final Set<String> candidateVmIds, final boolean targetPowerState) {
      Map<String, HostInfo> hostMap = new HashMap<String, HostInfo>();
      
      if (candidateVmIds.isEmpty()) {
         return null;
      }

      ClusterMap clusterMap = null;
      try {
         clusterMap = getAndReadLockClusterMap();
         for (String vmId : candidateVmIds) {
            String hostId = clusterMap.getHostIdForVm(vmId);
            if (hostId != null) {
               HostInfo hostInfo = hostMap.get(hostId);
               if (hostInfo == null) {
                  hostInfo = new HostInfo(hostId);
                  hostMap.put(hostId, hostInfo);
               }
               hostInfo._candidates.add(vmId);
            }
         }
         for (Entry<String, HostInfo> entry : hostMap.entrySet()) {
            Set<String> allVmsOnHostInPowerState = clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, entry.getKey(), true);
            if (allVmsOnHostInPowerState != null) {
               entry.getValue()._on = allVmsOnHostInPowerState.size();
            }
            Set<String> candidateVMs = entry.getValue()._candidates;
            _log.info("found "+candidateVMs.size()+" candidate VMs on host "+entry.getKey());
            for (String id : candidateVMs) {
               _log.info("candidate VM on "+entry.getKey()+": <%V"+id);
            }
         }
      } finally {
         unlockClusterMap(clusterMap);
      }
      
      return hostMap;
   }
   
   private Queue<HostInfo> orderHosts(Map<String, HostInfo> hostMap, final boolean targetPowerState) {
      PriorityQueue<HostInfo> orderedHosts = new PriorityQueue<HostInfo>(hostMap.size(), new Comparator<HostInfo>() {
         @Override
         public int compare(final HostInfo a, final HostInfo b) { 
            return targetPowerState ? a._on - b._on : b._on - a._on; 
         }
      });
      
      orderedHosts.addAll(hostMap.values());
      return orderedHosts;
   }

   private Set<RankedVM> rankVMsForTargetPowerState(String clusterId, Set<String> candidateVmIds, final boolean targetPowerState) {
      if (candidateVmIds != null) {
         Map<String, HostInfo> organizedHosts = organizeVMsByHost(clusterId, candidateVmIds, targetPowerState);
         if (organizedHosts != null) {
            Queue<HostInfo> orderedHosts = orderHosts(organizedHosts, targetPowerState);
            return rankVMs(orderedHosts, targetPowerState);
         }
      }
      return new HashSet<RankedVM>();
   }

   @Override
   public Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      return rankVMsForTargetPowerState(clusterId, candidateVmIds, true);
   }

   @Override
   public Set<RankedVM> rankVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      return rankVMsForTargetPowerState(clusterId, candidateVmIds, false);
   }

   @Override
   public Set<String> chooseVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      return null;
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      return null;
   }
   
}
