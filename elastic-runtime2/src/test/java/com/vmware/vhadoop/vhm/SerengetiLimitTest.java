package com.vmware.vhadoop.vhm;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.junit.Test;

import com.vmware.vhadoop.model.scenarios.BasicScenario;
import com.vmware.vhadoop.model.scenarios.Serengeti;
import com.vmware.vhadoop.model.scenarios.Serengeti.Master;
import com.vmware.vhadoop.vhm.model.vcenter.Host;

public class SerengetiLimitTest extends ModelTestBase {
   private static Logger _log = Logger.getLogger("SerengetiLimitTest");

   public SerengetiLimitTest() throws IOException, ClassNotFoundException {
      super(_log);
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

      /* perform the basic test setup that ModelTestBase depends on */
      _vCenter = BasicScenario.getVCenter(numberOfHosts + 1, 0);
      _serengeti = new Serengeti(getClass().getName()+"-vApp", _vCenter);

      /* set the general timeout for the test as a whole */
      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);

      /* create a cluster to work with */
      Master clusterA = _serengeti.createCluster(clusterId);

      @SuppressWarnings("unchecked")
      List<Host> hosts = (List<Host>) _vCenter.get(Host.class);
      for (Host host : hosts) {
         if (clusterA.getHost() == null) {
            /* master VMs need a host too */
            host.add(clusterA);
         } else {
            clusterA.createComputeNodes(computeNodesPerHost, host);
         }
      }

      /* power on the master node */
      clusterA.powerOn();

      /* start the system */
      startVHM();

      /* wait for VHM to register the VMs */
      assertClusterMapVMsInPowerState("register VMs in cluster map", clusterId, numberOfComputeNodes, false, timeout());

      /* register the cluster as an event producer */
      _vhm.registerEventProducer(clusterA);

      /* power on the compute nodes */
      clusterA.setTargetComputeNodeNum(numberOfComputeNodes);
      assertActualVMsInPowerState("power on all VMs", clusterA, numberOfComputeNodes, false, timeout());

      /* power off the compute nodes */
      clusterA.setTargetComputeNodeNum(0);
      assertActualVMsInPowerState("power off all VMs", clusterA, numberOfComputeNodes, false, timeout());

      _vhm.stop(true);
   }
}
