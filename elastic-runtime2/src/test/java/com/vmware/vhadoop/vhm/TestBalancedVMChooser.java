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

package com.vmware.vhadoop.vhm;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser.RankedVM;
import com.vmware.vhadoop.vhm.strategy.BalancedVMChooser;

import static org.junit.Assert.*;

public class TestBalancedVMChooser extends AbstractJUnitTest {
   BalancedVMChooser _chooser;
   StandaloneSimpleClusterMap _map;
   ClusterMapReader _parentClusterMapReader;

   @Before
   public void init() {
      _chooser = new BalancedVMChooser();
      _map = new StandaloneSimpleClusterMap(true);
      _chooser.initialize(getTestClusterMapReader(_map));
   }
   
   @After
   public void destroy() {
      MultipleReaderSingleWriterClusterMapAccess.destroy();
   }

   @Test
   public void testDisableAll() {
      String clusters[] = _map.getAllKnownClusterIds();

      if (clusters != null) {
         for (String clusterId : clusters) {
            Set<String> candidateVmIds = _map.listComputeVMsForClusterAndPowerState(clusterId, true);
            Set<RankedVM> rankedVMs = _chooser.rankVMsToDisable(clusterId, candidateVmIds);
            assertEquals(candidateVmIds.size(), rankedVMs.size());
         }
      }
   }

   @Test
   public void testEnableAll() {
      String clusters[] = _map.getAllKnownClusterIds();

      if (clusters != null) {
         for (String clusterId : clusters) {
            Set<String> candidateVmIds = _map.listComputeVMsForClusterAndPowerState(clusterId, false);
            Set<RankedVM> rankedVMs = _chooser.rankVMsToEnable(clusterId, candidateVmIds);
            assertEquals(candidateVmIds.size(), rankedVMs.size());
         }
      }
   }
   
   private void chooseVMsAndSetPowerState(String clusterId, int numToChoose, Integer expectedCandidateSize, boolean targetPowerState) {
      Set<String> candidateVmIds = _map.listComputeVMsForClusterAndPowerState(clusterId, !targetPowerState);
      if (expectedCandidateSize != null) {
         assertEquals((long)expectedCandidateSize, candidateVmIds.size());
      }
      Set<RankedVM> rankedVMs = targetPowerState ? 
            _chooser.rankVMsToEnable(clusterId, candidateVmIds) :
            _chooser.rankVMsToDisable(clusterId, candidateVmIds);

      PriorityQueue<RankedVM> orderedQueue = new PriorityQueue<RankedVM>(rankedVMs);
      RankedVM current = null;
      for (int cntr=0; (cntr < numToChoose) && ((current = orderedQueue.poll()) != null); cntr++) {
         assertEquals(!targetPowerState, _map.setPowerStateForVM(current.getVmId(), targetPowerState));
      }
   }

   @Test
   public void testIncrementalEnableAll() {
      String clusterId = "clusterA";
      
      _map.clearMap();
      int i = 0;
      for (i = 0; i < 7; i++) {     /* 7 powered on */
         _map.addVMToMap("vm"+i, clusterId, "hostX", true);
      }
      for (; i < 13; i++) {         /* 6 powered off */
         _map.addVMToMap("vm"+i, clusterId, "hostX", false);
      }
      for (; i < 20; i++) {         /* 7 powered off */
         _map.addVMToMap("vm"+i, clusterId, "hostY", false);
      }

      /* choose and power on 6, should all be host Y */
      chooseVMsAndSetPowerState(clusterId, 6, 13, true);
      
      /* power off 3 on hostX */
      for (i = 0; i < 3; i++) {
         assertTrue(_map.setPowerStateForVM("vm"+i, false));
      }

      /* now hostX should have 4 on, and hostY should have 6, power on 3 should have 2 on hostX and 1 on either  */
      Set<String> candidateVmIds = _map.listComputeVMsForClusterAndPowerState(clusterId, false);
      Set<RankedVM> rankedVMs = _chooser.rankVMsToEnable(clusterId, candidateVmIds);
      assertEquals(10, rankedVMs.size());
      
      Iterator<RankedVM> iterator = rankedVMs.iterator();
      int x = 0, y = 0;
      for (int cntr=0; (cntr < 3) && (iterator.hasNext()); cntr++) {
         RankedVM vm = iterator.next();
         String hostId = _map.getHostIdForVm(vm.getVmId());
         if (hostId.equals("hostX")) {
            x++;
         } else {
            y++;
         }
      }

      assertTrue("expected 2 or 3 VMs powered on on hostX", x >= 2);
      assertTrue("expected at most 1 VM powered on on hostY", y <= 1);
   }

   /* 19 powered on and 24 powered off:
    *   Host X: 6 on; 3 off; 9 total
    *   Host Y: 10 on; 0 off; 10 total 
    *   Host A: 1 on; 10 off; 11 total 
    *   Host B: 1 on; 11 off; 11 total 
    *   Host C: 1 on; 0 off: 1 total */
   private void createUnevenDistribution() {
      String clusterId = "clusterA";
      _map.clearMap();
      int vm = 0;
      for (int i = 0; i < 6; i++, vm++) {
         _map.addVMToMap("vm"+vm, clusterId, "hostX", true);
      }
      for (int i = 0; i < 3; i++, vm++) {
         _map.addVMToMap("vm"+vm, clusterId, "hostX", false);
      }
      for (int i = 0; i < 10; i++, vm++) {
         _map.addVMToMap("vm"+vm, clusterId, "hostY", true);
      }

      _map.addVMToMap("vm"+vm++, clusterId, "hostA", true);

      for (int i = 0; i < 10; i++, vm++) {
         _map.addVMToMap("vm"+vm, clusterId, "hostA", false);
      }

      _map.addVMToMap("vm"+vm++, clusterId, "hostB", true);
      for (int i = 0; i < 11; i++, vm++) {
         _map.addVMToMap("vm"+vm, clusterId, "hostB", false);
      }
      _map.addVMToMap("vm"+vm++, clusterId, "hostC", true);
   }

   @Test
   public void testUnevenDistributionPowerOff() {
      String clusterId = "clusterA";
      createUnevenDistribution();

      /* Power off 9 */
      chooseVMsAndSetPowerState(clusterId, 9, 19, false);

      assertTrue("Expected no change in hostA", _map.listComputeVMsForClusterHostAndPowerState(clusterId, "hostA", true).size() == 1);
      assertTrue("Expected no change in hostB", _map.listComputeVMsForClusterHostAndPowerState(clusterId, "hostB", true).size() == 1);
      assertTrue("Expected no change in hostC", _map.listComputeVMsForClusterHostAndPowerState(clusterId, "hostC", true).size() == 1);

      int num = _map.listComputeVMsForClusterHostAndPowerState(clusterId, "hostX", true).size();
      assertTrue("Expected hostX to have 3 or 4 VMs powered on, found "+num, num == 3 || num == 4);

      num = _map.listComputeVMsForClusterHostAndPowerState(clusterId, "hostY", true).size();
      assertTrue("Expected hostY to have 3 or 4 VMs powered on, found "+num, num == 3 || num == 4);
   }

   @Test
   public void testUnevenDistributionPowerOffAndOn() {
      String clusterId = "clusterA";
      createUnevenDistribution();

      /* Power off 9 */
      System.out.println("*** POWER OFF 9 ***");
      chooseVMsAndSetPowerState(clusterId, 9, 19, false);
      /* Power on 11 */
      System.out.println("*** POWER ON 11 ***");
      chooseVMsAndSetPowerState(clusterId, 11, 33, true);

      String hosts[] = new String[] { "hostX", "hostY", "hostA", "hostB"} ;
      for (String host : hosts) {
         int num = _map.listComputeVMsForClusterHostAndPowerState(clusterId, host, true).size();
         assertEquals("Expected "+host+" to have 5 VMs powered on, found "+num, 5, num);
      }

      int num = _map.listComputeVMsForClusterHostAndPowerState(clusterId, "hostC", true).size();
      assertEquals("Expected hostC to have 1 VMs powered on, found "+num, 1, num);
   }
}
