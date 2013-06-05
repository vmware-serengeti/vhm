package com.vmware.vhadoop.vhm;

import org.junit.Test;

import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.Master;

public class SerengetiLimitTest extends AbstractSerengetiTestBase {
   /**
    * Tests the auto expand case
    */
   @Test
   public void test() {
      final int numberOfHosts = 2;
      final int computeNodesPerHost = 2;
      String clusterName = "limitTestPowerAllOnAllOff";

      /* general test setup */
      setup(numberOfHosts);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      /* set the general timeout for the test as a whole */
      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);

      /* power on the compute nodes */
      cluster.setTargetComputeNodeNum(cluster.availableComputeNodes());
      assertActualVMsInPowerState("power on all VMs", cluster, cluster.availableComputeNodes(), true);

      /* power off the compute nodes */
      cluster.setTargetComputeNodeNum(0);
      assertActualVMsInPowerState("power off all VMs", cluster, cluster.availableComputeNodes(), false);
   }
}
