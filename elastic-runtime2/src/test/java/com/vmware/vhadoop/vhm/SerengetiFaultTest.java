package com.vmware.vhadoop.vhm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.vmware.vhadoop.vhm.model.scenarios.FaultInjectionSerengeti.Master;

public class SerengetiFaultTest extends AbstractFaultInjectionSerengetiTestBase {
   /**
    * Tests diagnostic messages in recommission cases
    */
   @Test
   public void testBasicRecommissionError() {
      final int numberOfHosts = 2;
      final int computeNodesPerHost = 2;
      String clusterName = "faultInjectionRecommission";

      /* general test setup */
      setup(numberOfHosts);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      /* set the general timeout for the faults */
      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);

      /* queue faults */
      String fault = "Injected error, failure to enable compute node";
      cluster.queueRecommissionFailure(fault);
      /* elicit faults */
      cluster.setTargetComputeNodeNum(2);
      Map<String,String> results = cluster.getResponses(timeout());

      String response = results.get(fault);
      assertNotNull("Expected detail message in response to recommission failure", response);
      assertTrue("Expected recommission error message to start with injected fault string", response.startsWith(fault));

      assertVMsInPowerState("expected VMs to power on regardless", cluster, 2, true);
      assertEquals("expected one compute node to be enabled", 1, cluster.numberComputeNodesInState(true));
   }

   /**
    * Tests diagnostic messages in recommission cases
    */
   @Test
   public void testBasicErrorDecommissionError() {
      final int numberOfHosts = 2;
      final int computeNodesPerHost = 2;
      String clusterName = "faultInjectionDecommission";

      /* general test setup */
      setup(numberOfHosts);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      /* set the general timeout for the faults */
      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);

      cluster.setTargetComputeNodeNum(2);
      assertVMsInPowerState("expected VMs to power on regardless", cluster, 2, true);

      /* queue faults */
      String fault = "Injected error, failed to disable compute node";
      cluster.queueDecommissionFailure(fault);

      cluster.setTargetComputeNodeNum(0);

      /* elicit faults */
      Map<String,String> results = cluster.getResponses(timeout());

      String response = results.get(fault);
      assertNotNull("Expected detail message in response to decommission failure", response);
      assertTrue("Expected decommission error message to start with injected fault string", response.startsWith(fault));

      assertVMsInPowerState("expected VMs to power off regardless", cluster, 0, true);
      assertEquals("expected one compute node to still be enabled", 1, cluster.numberComputeNodesInState(true));
   }

   /**
    * Tests diagnostic messages in recommission cases
    */
   @Test
   public void testInterleavedSuccessError() {
      final int numberOfHosts = 2;
      final int computeNodesPerHost = 3;
      String clusterName = "faultInjectionIntermittent";

      /* general test setup */
      setup(numberOfHosts);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      /* set the general timeout for the faults */
      setTimeout((computeNodesPerHost * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME);

      for (int i = 0; i < 6; i++) {
         boolean inject = i % 2 != 0;
         if (inject) {
            /* queue faults */
            String fault = "Injected error "+i+", failed to enable compute node";
            cluster.queueRecommissionFailure(fault);
         }

         cluster.setTargetComputeNodeNum(i);

         assertVMsInPowerState("expected VMs to power on regardless", cluster, i, true);
         assertEquals("expected compute node to fail enabling", i/2, cluster.numberComputeNodesInState(true));
      }

      /* check faults */
      Map<String,String> results = cluster.getResponses(timeout());
      for (String injected : results.keySet()) {
         String response = results.get(injected);
         assertNotNull("Expected detail message in response to failure", response);
         assertTrue("Expected error message to start with injected fault string", response.startsWith(injected));
      }
   }
}
