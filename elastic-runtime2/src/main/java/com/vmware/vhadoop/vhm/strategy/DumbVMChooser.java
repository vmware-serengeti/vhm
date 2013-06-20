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
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class DumbVMChooser extends AbstractClusterMapReader implements VMChooser {
   private static final Logger _log = Logger.getLogger(DumbVMChooser.class.getName());

   protected Set<String> chooseVMs(final Set<String> vms, int delta, final boolean targetPowerState) {
      delta = Math.abs(delta);

      Set<String> result = new HashSet<String>();
      Iterator<String> iterator = vms.iterator();
      for (int i=0; i<delta && iterator.hasNext(); i++) {
         String vm = iterator.next();
         _log.info("DumbVMChooser adding VM "+vm+" to results");
         result.add(vm);
      }
      return result;
   }

   public Set<String> chooseVMs(final String clusterId, final int delta, final boolean targetPowerState) {
      _log.info("DumbVMChooser choosing VMs for cluster "+clusterId+" where delta="+delta+", powerState="+!targetPowerState);

      Set<String> vmIds = null;
      ClusterMap clusterMap = getAndReadLockClusterMap();
      try {
         vmIds = clusterMap.listComputeVMsForClusterAndPowerState(clusterId, !targetPowerState);
      } finally {
         unlockClusterMap(clusterMap);
      }

      if (vmIds != null) {
         return chooseVMs(vmIds, delta, targetPowerState);
      }
      return null;
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
   public String chooseVMToEnableOnHost(final Set<String> candidates) {
      /* Not implemented */
      return null;
   }

   @Override
   public String chooseVMToDisableOnHost(final Set<String> candidates) {
      /* Not implemented */
      return null;
   }

   @Override
   public Set<String> chooseVMsToEnable(final Set<String> candidates, final int delta) {
      return chooseVMs(candidates, delta, true);
   }

   @Override
   public Set<String> chooseVMsToDisable(final Set<String> candidates, final int delta) {
      return chooseVMs(candidates, delta, false);
   }
}
