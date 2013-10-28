package com.vmware.vhadoop.vhm;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import static junit.framework.Assert.*;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.strategy.VMChooser;

public class VMChooserTests {
   
   private abstract class TestVMChooser extends AbstractClusterMapReader implements VMChooser {
      
      Set<String> _poweredOn = new HashSet<String>();
      Set<String> _poweredOff = new HashSet<String>();
      
      abstract boolean acceptVM(String vmId, boolean powerState);
      
      abstract int rankVM(String vmId, boolean powerState);
      
      public void addPoweredOnVMsForCluster(String clusterId, Set<String> vmIds) {
         _poweredOn.addAll(vmIds);
      }
      
      public void addPoweredOffVMsForCluster(String clusterId, Set<String> vmIds) {
         _poweredOff.addAll(vmIds);
      }

      @Override
      public Set<String> chooseVMsToEnable(String clusterId, int delta) {
         Set<String> result = new HashSet<String>();
         int cntr = Math.abs(delta);
         for (String vmId : _poweredOff) {
            if ((acceptVM(vmId, false) && (cntr-- > 0))) {
               result.add(vmId);
            }
         }
         return result;
      }

      @Override
      public Set<String> chooseVMsToDisable(String clusterId, int delta) {
         Set<String> result = new HashSet<String>();
         int cntr = Math.abs(delta);
         for (String vmId : _poweredOn) {
            if ((acceptVM(vmId, true) && (cntr-- > 0))) {
               result.add(vmId);
            }
         }
         return result;
      }

      @Override
      public Set<RankedVM> rankVMsToEnable(String clusterId) {
         Set<RankedVM> result = new HashSet<RankedVM>();
         for (String vmId : _poweredOff) {
            int rank = rankVM(vmId, false);
            result.add(new RankedVM(vmId, rank));
         }
         return result;
      }

      @Override
      public Set<RankedVM> rankVMsToDisable(String clusterId) {
         Set<RankedVM> result = new HashSet<RankedVM>();
         for (String vmId : _poweredOn) {
            int rank = rankVM(vmId, true);
            result.add(new RankedVM(vmId, rank));
         }
         return result;
      }
      
   }
   
   @Test
   public void testTrivial() {
      TestVMChooser foo = new TestVMChooser() {
         @Override
         int rankVM(String vmId, boolean powerState) {
            char lastChar = vmId.charAt(vmId.length()-1);
            return (int)lastChar;
         }
         @Override
         boolean acceptVM(String vmId, boolean powerState) {
            return (vmId.length() == 3);
         }
      };
      foo.addPoweredOffVMsForCluster("notUsed", new HashSet<String>(Arrays.asList(new String[]{"vm1", "vma2"})));
      foo.addPoweredOnVMsForCluster("notUsed", new HashSet<String>(Arrays.asList(new String[]{"vm3", "vma4"})));
      assertEquals(1, foo.chooseVMsToEnable("notUsed", 2).size());
      assertEquals("vm1", foo.chooseVMsToEnable("notUsed", 2).iterator().next());
      assertEquals(1, foo.chooseVMsToDisable("notUsed", 2).size());
      assertEquals("vm3", foo.chooseVMsToDisable("notUsed", 2).iterator().next());
   }
   
}
