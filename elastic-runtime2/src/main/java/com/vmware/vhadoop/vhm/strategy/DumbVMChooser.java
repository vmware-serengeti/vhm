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

import com.vmware.vhadoop.api.vhm.strategy.VMChooser;

public class DumbVMChooser implements VMChooser {
   private static final Logger _log = Logger.getLogger(DumbVMChooser.class.getName());

   Set<String> chooseSubset(final Set<String> vms, int delta) {
      delta = Math.abs(delta);
      Set<String> result = new HashSet<String>();
      Iterator<String> iterator = vms.iterator();
      for (int i=0; i<delta && iterator.hasNext(); i++) {
         String vm = iterator.next();
         _log.info("DumbVMChooser adding VM <%V"+vm+"%V> to results");
         result.add(vm);
      }
      return result;
   }
   
   public Set<RankedVM> rankVMs(final String clusterId, final Set<String> candidateVmIds) {
      Set<RankedVM> result = new HashSet<RankedVM>();
      if (candidateVmIds == null) {
         return null;
      }
      for (String vmId : candidateVmIds) {
         _log.info("DumbVMChooser ranking VM <%V"+vmId+"%V> at 0");
         result.add(new RankedVM(vmId, 0));    /* All equal rank */
      }
      return result;
   }

   @Override
   public Set<String> chooseVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      return chooseSubset(candidateVmIds, Integer.MAX_VALUE);
   }

   @Override
   public Set<String> chooseVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      return chooseSubset(candidateVmIds, Integer.MAX_VALUE);
   }

   @Override
   public Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds) {
      return rankVMs(clusterId, candidateVmIds);
   }

   @Override
   public Set<RankedVM> rankVMsToDisable(String clusterId, Set<String> candidateVmIds) {
      return rankVMs(clusterId, candidateVmIds);
   }
}
