package com.vmware.vhadoop.vhm;

import java.util.Arrays;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import static org.junit.Assert.*;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.strategy.VMChooser;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser.RankedVM;

public class VMChooserTests {
   
   private abstract class TestVMChooser implements VMChooser {
            
      abstract boolean acceptVM(String vmId, boolean powerState);
      
      abstract int rankVM(String vmId, boolean targetPowerState);
      
      @Override
      public Set<String> chooseVMsToEnable(String clusterId, Set<String> candidateVmIds) {
         Set<String> result = new HashSet<String>();
         for (String vmId : candidateVmIds) {
            if (acceptVM(vmId, true)) {
               result.add(vmId);
            }
         }
         return result;
      }

      @Override
      public Set<String> chooseVMsToDisable(String clusterId, Set<String> candidateVmIds) {
         Set<String> result = new HashSet<String>();
         for (String vmId : candidateVmIds) {
            if (acceptVM(vmId, false)) {
               result.add(vmId);
            }
         }
         return result;
      }

      @Override
      public Set<RankedVM> rankVMsToEnable(String clusterId, Set<String> candidateVmIds) {
         Set<RankedVM> result = new HashSet<RankedVM>();
         for (String vmId : candidateVmIds) {
            result.add(new RankedVM(vmId, rankVM(vmId, true)));
         }
         return result;
      }

      @Override
      public Set<RankedVM> rankVMsToDisable(String clusterId, Set<String> candidateVmIds) {
         Set<RankedVM> result = new HashSet<RankedVM>();
         for (String vmId : candidateVmIds) {
            result.add(new RankedVM(vmId, rankVM(vmId, false)));
         }
         return result;
      }
      
   }
   
   @Test
   public void testTrivial() {
      String clusterId = "notUsed";
      TestVMChooser vmChooser = new TestVMChooser() {
         @Override
         /* Rank VMs based on the trailing character */
         int rankVM(String vmId, boolean powerState) {
            char lastChar = vmId.charAt(vmId.length()-1);
            return lastChar;
         }
         @Override
         /* Choose VMs only if their ID is 3 chars long */
         boolean acceptVM(String vmId, boolean powerState) {
            return (vmId.length() == 3);
         }
      };
      
      Set<String> poweredOffVMs = new HashSet<String>(Arrays.asList(new String[]{"vm2", "vma1"}));
      Set<String> poweredOnVMs = new HashSet<String>(Arrays.asList(new String[]{"vm4", "vma3"}));

      Set<String> chosen = vmChooser.chooseVMsToEnable(clusterId, poweredOffVMs);
      assertEquals(1, chosen.size());
      assertEquals("vm2", chosen.iterator().next());
      
      chosen = vmChooser.chooseVMsToDisable(clusterId, poweredOnVMs);
      assertEquals(1, chosen.size());
      assertEquals("vm4", chosen.iterator().next());
      
      Set<RankedVM> ranked = vmChooser.rankVMsToEnable(clusterId, poweredOffVMs);
      assertEquals(2, ranked.size());
      assertEquals("vma1", RankedVM.selectLowestRankedIds(ranked, 1).iterator().next());
      
      ranked = vmChooser.rankVMsToDisable(clusterId, poweredOnVMs);
      assertEquals(2, ranked.size());
      assertEquals("vma3", RankedVM.selectLowestRankedIds(ranked, 1).iterator().next());
   }
   
   @Test
   public void testCombineRankings() {
      String clusterId = "notUsed";
      TestVMChooser vmChooser1 = new TestVMChooser() {
         @Override
         /* Rank VMs based on the literal value of the trailing character */
         int rankVM(String vmId, boolean powerState) {
            char lastChar = vmId.charAt(vmId.length()-1);
            return Integer.parseInt(""+lastChar);
         }
         @Override
         /* Choose VMs only if their ID is 4 chars long */
         boolean acceptVM(String vmId, boolean powerState) {
            return (vmId.length() == 4);
         }
      };

      TestVMChooser vmChooser2 = new TestVMChooser() {
         @Override
         /* Rank VMs based on the literal value of the first character */
         int rankVM(String vmId, boolean powerState) {
            char firstChar = vmId.charAt(0);
            return Integer.parseInt(""+firstChar);
         }
         @Override
         /* Choose VMs only if their ID is not 4 chars long */
         boolean acceptVM(String vmId, boolean powerState) {
            return (vmId.length() != 4);
         }
      };
      
      Set<String> poweredOffVMs = new HashSet<String>(Arrays.asList(new String[]{"9vm0", "8vm1", "7vm2", "4vma5", "3vma6", "0vma9"}));
      Set<String> poweredOnVMs = new HashSet<String>(Arrays.asList(new String[]{"6vm3", "5vm4", "2vma7", "1vma8"}));
      
      Set<RankedVM> ranked1 = vmChooser1.rankVMsToEnable(clusterId, poweredOffVMs);
      assertEquals(6, ranked1.size());
      assertEquals("9vm0", RankedVM.selectLowestRankedIds(ranked1, 1).iterator().next());
      
      Set<RankedVM> ranked2 = vmChooser2.rankVMsToEnable(clusterId, poweredOffVMs);
      assertEquals(6, ranked2.size());
      assertEquals("0vma9", RankedVM.selectLowestRankedIds(ranked2, 1).iterator().next());
      
      Set<RankedVM> result = RankedVM.combine(ranked1, null);
      assertEquals(6, result.size());

      result = RankedVM.combine(null, ranked2);
      assertEquals(6, result.size());

      result = RankedVM.combine(ranked1, ranked2);
      for (RankedVM rankedVM : result) {
         assertEquals(9, rankedVM.getRank());
      }
   }
   
   @Test
   public void testFlattenRankValues() {
      String clusterId = "notUsed";
      TestVMChooser vmChooser = new TestVMChooser() {
         @Override
         /* Rank VMs based on a multiple of the value of the trailing character */
         int rankVM(String vmId, boolean powerState) {
            char lastChar = vmId.charAt(vmId.length()-1);
            return Integer.parseInt(""+lastChar) * 13;
         }
         @Override
         /* Choose VMs only if their ID is 4 chars long */
         boolean acceptVM(String vmId, boolean powerState) {
            return false;
         }
      };

      Set<String> poweredOffVMs = new HashSet<String>(Arrays.asList(new String[]{"vm0", "vm5", "vma5", "vm7", "vma7", "vmb7", "vm9"}));
      Set<RankedVM> ranked = vmChooser.rankVMsToEnable(clusterId, poweredOffVMs);
      assertEquals(7, ranked.size());
      
      Set<RankedVM> flattened = RankedVM.flattenRankValues(ranked);
      assertEquals(7, flattened.size());
      int[] expected = new int[]{0, 1, 1, 2, 2, 2, 3};
      PriorityQueue<RankedVM> orderedQueue = new PriorityQueue<RankedVM>(flattened);
      for (int cntr = 0; cntr < expected.length; cntr++) {
         assertEquals(expected[cntr], orderedQueue.poll().getRank());
      }
   }
   
}
