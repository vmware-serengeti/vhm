package com.vmware.vhadoop.vhm;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.vmware.vhadoop.vhm.model.scenarios.FaultInjectionSerengeti.Master;

public class SerengetiFaultTest extends AbstractFaultInjectionSerengetiTestBase {
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
//      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);
//
//      /* make sure we're all functioning without faults */
//      cluster.setTargetComputeNodeNum(cluster.availableComputeNodes());
//      assertVMsInPowerState("power on all VMs", cluster, cluster.availableComputeNodes(), true);
//      cluster.setTargetComputeNodeNum(0);
//      assertVMsInPowerState("power off all VMs", cluster, cluster.availableComputeNodes(), false);

      /* set the general timeout for the faults */
      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);

      /* queue faults */
      String fault = "Compute node failed to start";
      cluster.queueRecommissionFailure(fault);
      /* elicit faults */
      cluster.setTargetComputeNodeNum(1);
      Map<String,String> results = cluster.getResponses(timeout());

      assertEquals("Expected recommission error message to match injected fault", fault, results.get(fault));
   }
}
