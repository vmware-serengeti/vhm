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

package com.vmware.vhadoop.api.vhm.strategy;

import java.util.Comparator;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public interface VMChooser extends ClusterMapReader {

   public class RankedVM implements Comparator<RankedVM> {
      String _vmId;
      Integer _rank;
      
      public static void combine(Set<RankedVM> combineInto, Set<RankedVM> input) {
         for (RankedVM fromCombineInto : combineInto) {
            for (RankedVM fromInput : input) {
               if (fromCombineInto.combine(fromInput)) {
                  continue;
               }
            }
         }
         input.removeAll(combineInto);
         combineInto.addAll(input);
      }
      
      public RankedVM(String vmId, int rank) {
         _vmId = vmId;
         _rank = rank;
      }
      
      public boolean combine(RankedVM combineWith) {
         if (combineWith._vmId.equals(_vmId)) {
            return false;
         }
         _rank += combineWith._rank;
         return true;
      }

      public String getVmId() {
         return _vmId;
      }

      @Override
      public int compare(RankedVM arg0, RankedVM arg1) {
         return arg0._rank - arg1._rank;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((_vmId == null) ? 0 : _vmId.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         RankedVM other = (RankedVM) obj;
         if (_vmId == null) {
            if (other._vmId != null)
               return false;
         } else if (!_vmId.equals(other._vmId))
            return false;
         return true;
      }
      
   }
   
   /**
    * Selects VMs to enable from the specified cluster in no particular order. The logic determining which VMs is provided by implementors.
    * If there is a reason why VMs should not be chosen, other than already being powered on, they should not be returned. 
    *   This means that the method is not guaranteed to return a set of delta elements
    * @param clusterId - the target cluster
    * @param delta - the number of VMs to enable
    * @return - set of VM ids to enable or null if not implemented
    */
   Set<String> chooseVMsToEnable(String clusterId, int delta);

   /**
    * Selects VMs to disable from the specified cluster in no particular order. The logic determining which VMs is provided by implementors.
    * If there is a reason why VMs should not be chosen, other than already being powered off, they should not be returned. 
    *   This means that the method is not guaranteed to return a set of delta elements.
    * @param clusterId - the target cluster
    * @param delta - the number of VMs to disable
    * @return - set of VM ids to disable or null if not implemented
    */
   Set<String> chooseVMsToDisable(String clusterId, int delta);

   /**
    * Ranks VMs to disable from the specified cluster. All powered-off VMs in the cluster should be returned
    * @param clusterId - the target cluster
    * @return - ordered set of eligible VM IDs or null if not implemented
    */
   Set<RankedVM> rankVMsToEnable(String clusterId);
   
   /**
    * Ranks VMs to disable from the specified cluster. All powered-on VMs in the cluster should be returned
    * @param clusterId - the target cluster
    * @return - ordered set of eligible VM IDs or null if not implemented
    */
   Set<RankedVM> rankVMsToDisable(String clusterId);
}
