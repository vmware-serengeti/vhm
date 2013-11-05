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

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * VMChooser represents an abstraction for choosing VMs from a cluster to be enabled or disabled
 * A VMChooser must implement either a choosing strategy, a ranking strategy or both.
 * A strategy that is not implemented should return null, to differentiate from an empty set
 * A VMChooser is able to choose VMs based on its own particular private knowledge and this may be combined
 *   with the input from other VMChoosers
 */
public interface VMChooser {

   public class RankedVM implements Comparable<RankedVM> {
      String _vmId;
      Integer _rank;

      /* The result should contain the superset of both input1 and input2
       * If input1 is null, input2 is returned and vice versa
       * If both are null, null is returned */
      public static Set<RankedVM> combine(Set<RankedVM> input1, Set<RankedVM> input2) {
         if (input1 == null) {
            if (input2 != null) {
               return input2;
            } else {
               return null;
            }
         } else if (input2 == null) {
            return input1;
         }
         Set<RankedVM> copyInput1 = new HashSet<RankedVM>(input1);
         for (RankedVM fromInput2 : input2) {
            for (RankedVM fromInput1 : copyInput1) {
               if (fromInput1.combine(fromInput2)) {
                  break;
               }
            }
         }
         copyInput1.addAll(input2);
         return copyInput1;
      }

      /* Takes a set of ranked VMs and re-ranks them sequentially from 0 */
      public static Set<RankedVM> flattenRankValues(Set<RankedVM> toFlatten) {
         if (toFlatten == null) {
            return null;
         }
         int rank = -1;
         RankedVM current = null;
         RankedVM prev = null;
         Set<RankedVM> flattened = new HashSet<RankedVM>();
         PriorityQueue<RankedVM> orderedCopy = new PriorityQueue<RankedVM>(toFlatten); 
         while ((current = orderedCopy.poll()) != null) {
            if ((prev != null) && (current.getRank() == prev.getRank())) {
               flattened.add(new RankedVM(current._vmId, rank));
            } else {
               flattened.add(new RankedVM(current._vmId, ++rank));
            }
            prev = current;
         }
         return flattened;
      }
      
      /* Orders the candidates passed in by ranking and selects the lowest ranked candidates 
       * Returns null if candidates is null, else a Set */
      public static Set<String> selectLowestRankedIds(Set<RankedVM> candidates, int numToChoose) {
         if (candidates == null) {
            return null;
         }
         PriorityQueue<RankedVM> orderedQueue = new PriorityQueue<RankedVM>(candidates);
         Set<String> result = new HashSet<String>();
         RankedVM current = null;
         for (int i=0; (i < numToChoose) && ((current = orderedQueue.poll()) != null); i++) {
            result.add(current.getVmId());
         }
         return result;
      }

      /* Selects a single candidate from the set. Returns null if the set is null or empty */
      public static String selectLowestRankedId(Set<RankedVM> candidates) {
         if ((candidates == null || candidates.isEmpty())) {
            return null;
         }
         PriorityQueue<RankedVM> orderedQueue = new PriorityQueue<RankedVM>(candidates);
         return orderedQueue.poll().getVmId();
      }
      
      public RankedVM(String vmId, int rank) {
         _vmId = vmId;
         _rank = rank;
      }
      
      public boolean combine(RankedVM combineWith) {
         if (!combineWith._vmId.equals(_vmId)) {
            return false;
         }
         _rank += combineWith._rank;
         return true;
      }

      public String getVmId() {
         return _vmId;
      }

      public int getRank() {
         return _rank;
      }

      @Override
      public int compareTo(RankedVM other) {
         return _rank - other._rank;
      }
      
      @Override
      public String toString() {
         return "<%V"+_vmId+"%V>="+_rank;
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
    * Selects VMs to enable from the specified candidates in no particular order. The logic determining which VMs is provided by implementors.
    * @param clusterId - the target cluster
    * @param candidateVmIds - the candidate VMs from which to choose, which should all belong to the specified cluster and should all be powered off
    * @return - set of VM ids deemed OK to enable or null if not implemented
    */
   Set<String> chooseVMsToEnable(String clusterId, Set<String> candidateVmIds);

   /**
    * Selects VMs to disable from the specified candidates in no particular order. The logic determining which VMs is provided by implementors.
    * @param clusterId - the target cluster
    * @param candidateVmIds - the candidate VMs from which to choose, which should all belong to the specified cluster
    * @return - set of VM ids deemed OK to disable or null if not implemented
    */
   Set<String> chooseVMsToDisable(String clusterId, Set<String> candidateVmIds);

   /**
    * Ranks VMs to enable from the specified cluster from the candidates provided
    * Ranking should be provided in the form of sequential digits from 0 to n
    * @param clusterId - the target cluster
    * @param candidateVmIds - the candidate VMs from which to choose, which should all belong to the specified cluster
    * @return - Set of VM IDs with associated ranking or null if not implemented
    */
   Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds);
   
   /**
    * Ranks VMs to disable from the specified cluster from the candidates provided
    * Ranking should be provided in the form of sequential digits from 0 to n
    * @param clusterId - the target cluster
    * @param candidateVmIds - the candidate VMs from which to choose, which should all belong to the specified cluster
    * @return - Set of VM IDs with associated ranking or null if not implemented
    */
   Set<RankedVM> rankVMsToDisable(String clusterId, Set<String> candidateVmIds);
}
