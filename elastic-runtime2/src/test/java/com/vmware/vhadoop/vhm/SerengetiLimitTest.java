package com.vmware.vhadoop.vhm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.model.Host;
import com.vmware.vhadoop.model.scenarios.BasicScenario;
import com.vmware.vhadoop.model.scenarios.Serengeti;

public class SerengetiLimitTest extends ModelTestBase {

   public SerengetiLimitTest() throws IOException, ClassNotFoundException {
      super();
   }

   /**
    * Tests the auto expand case
    */
   @Test
   public void test() {
      final int numberOfHosts = 2;
      final int computeNodesPerHost = 2;
      final int numberOfComputeNodes = numberOfHosts * computeNodesPerHost;
      String clusterId = "limitTestPowerAllOnAllOff";

      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);

      _orchestrator = BasicScenario.getOrchestrator(numberOfHosts, 0);

      /* create a serengeti setup */
      Serengeti serengeti = new Serengeti(getClass().getName()+"-vApp", _orchestrator);
      _clusterA = serengeti.createCluster(clusterId);

      @SuppressWarnings("unchecked")
      List<Host> hosts = (List<Host>) _orchestrator.get(Host.class);
      for (Host host : hosts) {
         _clusterA.createComputeNodes(computeNodesPerHost, host);
      }

      /* power on the master node */
      _clusterA.powerOn();

      /* start the system */
      startVHM();

      /* register the cluster as an event producer. This will cause us to generate the limit instruction */
      _vhm.registerEventProducer(_clusterA);

      /* wait for VHM to register the VMs */
      Set<String> vms;
      do {
         try {
            Thread.sleep(500);
         } catch (InterruptedException e) {}
         ClusterMap map = getAndReadLockClusterMap();
         /* we really care about number of VMs in the cluster but we know that they're starting powered off at this point */
         vms = map.listComputeVMsForClusterAndPowerState(clusterId, false);
         unlockClusterMap(map);
      } while ((vms == null || vms.size() < numberOfComputeNodes) && timeout() > 0);

      assertNotNull("No VMs register in cluster map", vms);
      assertEquals("Not enough VMs registered in cluster map", numberOfComputeNodes, vms.size());

      /* power on the compute nodes */
      _clusterA.setMinInstances(numberOfComputeNodes);

      /* wait for all the VMs to be powered on */
      while (_clusterA.getCurrentInstances() < numberOfComputeNodes && timeout() >= 0) {
         _orchestrator.waitForConfigurationUpdate(timeout());
      }

      assertTrue("Test timed out waiting for minInstances to power on", _clusterA.getCurrentInstances() == numberOfComputeNodes);

      /* power off the compute nodes */
      _clusterA.setMinInstances(0);

      /* wait for all the VMs to be powered on */
      while (_clusterA.getCurrentInstances() > 0 && timeout() >= 0) {
         _orchestrator.waitForConfigurationUpdate(timeout());
      }

      assertTrue("Test timed out waiting for minInstances to power off", _clusterA.getCurrentInstances() == 0);

      _vhm.stop(true);
   }
}
