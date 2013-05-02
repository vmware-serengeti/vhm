package com.vmware.vhadoop.vhm;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.vmware.vhadoop.vhm.strategy.BalancedVMChooser;


public class TestBalancedVMChooser {
   BalancedVMChooser _chooser;
   StandaloneSimpleClusterMap _map;

   @Before
   public void init() {
      _chooser = new BalancedVMChooser();
      _map = new StandaloneSimpleClusterMap();
      _chooser.registerClusterMapAccess(_map, null);
   }

   @Test
   public void testDisableAll() {
      String clusters[] = _map.getAllKnownClusterIds();

      for (String cluster : clusters) {
         int delta = _map.listComputeVMsForClusterAndPowerState(cluster, true).size();
         Set<String> vms = _chooser.chooseVMsToDisable(cluster, delta);
         Assert.assertEquals(delta, vms.size());
      }
   }

   @Test
   public void testEnableAll() {
      String clusters[] = _map.getAllKnownClusterIds();

      for (String cluster : clusters) {
         int delta = _map.listComputeVMsForClusterAndPowerState(cluster, false).size();
         Set<String> vms = _chooser.chooseVMsToEnable(cluster, delta);
         Assert.assertEquals(delta, vms.size());
      }
   }

   @Test
   public void testIncrementalEnableAll() {
      _map.clearMap();
      int i = 0;
      for (i = 0; i < 7; i++) {
         _map.addVMToMap("vm"+i, "clusterA", "hostX", true);
      }
      for (; i < 13; i++) {
         _map.addVMToMap("vm"+i, "clusterA", "hostX", false);
      }
      for (; i < 20; i++) {
         _map.addVMToMap("vm"+i, "clusterA", "hostY", false);
      }

      /* power on 6, should all be host Y */
      for (i = 0; i < 6; i++) {
         Set<String> vms = _chooser.chooseVMsToEnable("clusterA", 1);
         Assert.assertEquals(1, vms.size());
         String vmid = vms.iterator().next();
         Assert.assertEquals("hostY", _map.getHostIdForVm(vmid));
         Assert.assertFalse(_map.setPowerStateForVM(vmid, true));
      }

      /* power off 3 on hostX */
      for (i = 0; i < 3; i++) {
         Assert.assertTrue(_map.setPowerStateForVM("vm"+i, false));
      }

      /* now hostX should have 4 on, and hostY should have 6, power on 3 should have 2 on hostX and 1 on either  */
      Set<String> vms = _chooser.chooseVMsToEnable("clusterA", 3);
      Assert.assertEquals(3, vms.size());
      int x = 0, y = 0;
      for (String vmid : vms) {
         String hostid = _map.getHostIdForVm(vmid);
         if (hostid.equals("hostX")) {
            x++;
         } else {
            y++;
         }
      }

      Assert.assertTrue("expected 2 or 3 VMs powered on on hostX", x >= 2);
      Assert.assertTrue("expected at most 1 VM powered on on hostY", y <= 1);
   }

   private void createUnevenDistribution() {
      _map.clearMap();
      int vm = 0;
      for (int i = 0; i < 6; i++, vm++) {
         _map.addVMToMap("vm"+vm, "clusterA", "hostX", true);
      }
      for (int i = 0; i < 3; i++, vm++) {
         _map.addVMToMap("vm"+vm, "clusterA", "hostX", false);
      }
      for (int i = 0; i < 10; i++, vm++) {
         _map.addVMToMap("vm"+vm, "clusterA", "hostY", true);
      }

      _map.addVMToMap("vm"+vm++, "clusterA", "hostA", true);

      for (int i = 0; i < 10; i++, vm++) {
         _map.addVMToMap("vm"+vm, "clusterA", "hostA", false);
      }

      _map.addVMToMap("vm"+vm++, "clusterA", "hostB", true);
      for (int i = 0; i < 10; i++, vm++) {
         _map.addVMToMap("vm"+vm, "clusterA", "hostB", false);
      }
      _map.addVMToMap("vm"+vm++, "clusterA", "hostB", false);
      _map.addVMToMap("vm"+vm++, "clusterA", "hostC", true);
   }

   @Test
   public void testUnevenDistributionPowerOff() {
      createUnevenDistribution();

      Set<String> vms = _chooser.chooseVMsToDisable("clusterA", 9);
      for (String vmid : vms) {
         Assert.assertTrue(_map.setPowerStateForVM(vmid, false));
      }

      Assert.assertTrue("Expected no change in hostA", _map.listComputeVMsForClusterHostAndPowerState("clusterA", "hostA", true).size() == 1);
      Assert.assertTrue("Expected no change in hostB", _map.listComputeVMsForClusterHostAndPowerState("clusterA", "hostB", true).size() == 1);
      Assert.assertTrue("Expected no change in hostC", _map.listComputeVMsForClusterHostAndPowerState("clusterA", "hostC", true).size() == 1);

      int num = _map.listComputeVMsForClusterHostAndPowerState("clusterA", "hostX", true).size();
      Assert.assertTrue("Expected hostX to have 3 or 4 VMs powered on, found "+num, num == 3 || num == 4);

      num = _map.listComputeVMsForClusterHostAndPowerState("clusterA", "hostY", true).size();
      Assert.assertTrue("Expected hostY to have 3 or 4 VMs powered on, found "+num, num == 3 || num == 4);
   }

   @Test
   public void testUnevenDistributionPowerOffAndOn() {
      createUnevenDistribution();

      Set<String> vms = _chooser.chooseVMsToDisable("clusterA", 9);
      for (String vmid : vms) {
         Assert.assertTrue(_map.setPowerStateForVM(vmid, false));
      }
      vms = _chooser.chooseVMsToEnable("clusterA", 11);
      for (String vmid : vms) {
         Assert.assertFalse(_map.setPowerStateForVM(vmid, true));
      }

      String hosts[] = new String[] { "hostX", "hostY", "hostA", "hostB"} ;
      for (String host : hosts) {
         int num = _map.listComputeVMsForClusterHostAndPowerState("clusterA", host, true).size();
         Assert.assertEquals("Expected "+host+" to have 5 VMs powered on, found "+num, 5, num);
      }

      int num = _map.listComputeVMsForClusterHostAndPowerState("clusterA", "hostC", true).size();
      Assert.assertEquals("Expected hostC to have 1 VMs powered on, found "+num, 1, num);
   }
}
