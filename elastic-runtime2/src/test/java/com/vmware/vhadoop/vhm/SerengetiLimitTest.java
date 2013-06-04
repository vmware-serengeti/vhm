package com.vmware.vhadoop.vhm;

import org.junit.Test;

import com.vmware.vhadoop.model.scenarios.Serengeti.Master;

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

//      ClusterMap map = getAndReadLockClusterMap();
//      /* we really care about number of VMs in the cluster but we know that they're starting powered off at this point */
//      Set<String> host0 = map.listComputeVMsForClusterHostAndPowerState(cluster.getClusterId(), "host0", false);
//      Set<String> host1 = map.listComputeVMsForClusterHostAndPowerState(cluster.getClusterId(), "host1", false);
//      unlockClusterMap(map);

//      assertEquals("checking VMs per host for "+cluster, computeNodesPerHost, host0.size());
//      assertEquals("checking VMs per host for "+cluster, computeNodesPerHost, host1.size());
//      assertEquals("checking VMs per host for "+cluster, computeNodesPerHost, host2.size());

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
