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

import org.junit.Test;

import com.vmware.vhadoop.vhm.model.scenarios.Master;

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
      assertVMsInPowerState("power on all VMs", cluster, cluster.availableComputeNodes(), true);

      /* power off the compute nodes */
      cluster.setTargetComputeNodeNum(0);
      assertVMsInPowerState("power off all VMs", cluster, cluster.availableComputeNodes(), false);
   }
}
