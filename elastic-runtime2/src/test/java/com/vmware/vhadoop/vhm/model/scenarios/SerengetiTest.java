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

package com.vmware.vhadoop.vhm.model.scenarios;

import static com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter.Metric.GRANTED;
import static com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter.Metric.READY;
import static com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter.Metric.USAGE;
import static org.junit.Assert.assertEquals;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.vhm.AbstractSerengetiTestBase;
import com.vmware.vhadoop.vhm.model.Allocation;
import com.vmware.vhadoop.vhm.model.api.ResourceType;
import com.vmware.vhadoop.vhm.model.hadoop.HadoopJob;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter.Metric;
import com.vmware.vhadoop.vhm.model.workloads.EndlessTaskGreedyJob;

public class SerengetiTest extends AbstractSerengetiTestBase
{
   private static Logger _log = Logger.getLogger(SerengetiTest.class.getName());

   public SerengetiTest() {
      Logger.getLogger("").setLevel(Level.FINER);
      Handler handler = Logger.getLogger("").getHandlers()[0];
      handler.setLevel(Level.FINER);
      handler.setFormatter(new LogFormatter());
   }

   private void logMetrics(Compute nodes[]) {
      for (Compute node : nodes) {
         if (!node.powerState()) {
            continue;
         }

         long metrics[] = _vCenter.getMetrics(node.name());
         StringBuilder sb = new StringBuilder(node.name());
         for (Metric metric : new Metric[] {USAGE, READY, GRANTED}) {
            sb.append("\t").append(metric).append(": ").append(metrics[metric.ordinal()]);
         }

         _log.info(sb.toString());
      }
   }

   @Test
   public void testJobDeploysAfterHardPowerCycle() {
      final int numberOfHosts = 2;
      final int computeNodesPerHost = 2;
      String clusterName = "serengetiTest";

      Allocation hostCapacity = Allocation.zeroed();
      hostCapacity.set(ResourceType.CPU, 4000);
      hostCapacity.set(ResourceType.MEMORY, 24000);

      /* general test setup */
      setup(numberOfHosts, hostCapacity);
      _vCenter.setMetricsInterval(200);
      _serengeti.setMaxLatency(500);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      /* power on all the nodes and enable them in serengeti */
      Compute nodes[] = cluster.getComputeNodes().toArray(new Compute[0]);

      for (Compute node : nodes) {
         _log.info("Powering on node "+node.name());
         node.powerOn();
         getApplication(cluster).enable(node.getHostname());
      }

      /* hard cycle vms */
      for (int i = 0; i < nodes.length; i++) {
         nodes[i].powerOff();
      }
      for (int i = 0; i < nodes.length; i++) {
         nodes[i].powerOn();
      }

      Allocation footprint = Allocation.zeroed();
      footprint.set(ResourceType.CPU, 4000);
      footprint.set(ResourceType.MEMORY, 2000);
      HadoopJob job = new EndlessTaskGreedyJob("greedyJob", numberOfHosts, footprint);

      /* start a job that should roll out to all of the nodes as they power on */
      getApplication(cluster).execute(job);

      while(!assertWaitEquals("greedy job should fill nodes", nodes.length, job.numberOfTasks(HadoopJob.Stage.PROCESSING), _serengeti.getMaxLatency()));

      /* wait for the serengeti max latency and stats interval to expire */
      long delay = Math.max(_vCenter.getMetricsInterval(), _serengeti.getMaxLatency());
      try {
         Thread.sleep(delay);
      } catch (InterruptedException e) {}

      logMetrics(nodes);
      for (Compute node : nodes) {
         assertEquals("cpu ready value is not correct for "+node.name(), 2000, _vCenter.getRawMetric(node.name(), READY).longValue());
      }
   }

   @Test
   public void test() {
      final int numberOfHosts = 1;
      final int computeNodesPerHost = 2;
      String clusterName = "serengetiTest";

      Allocation hostCapacity = Allocation.zeroed();
      hostCapacity.set(ResourceType.CPU, 4000);
      hostCapacity.set(ResourceType.MEMORY, 24000);

      /* general test setup */
      setup(numberOfHosts, hostCapacity);
      _vCenter.setMetricsInterval(200);
      _serengeti.setMaxLatency(500);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      /* power on all the nodes and enable them in serengeti */
      Compute nodes[] = cluster.getComputeNodes().toArray(new Compute[0]);

      Allocation footprint = Allocation.zeroed();
      footprint.set(ResourceType.CPU, 4000);
      footprint.set(ResourceType.MEMORY, 2000);
      HadoopJob job = new EndlessTaskGreedyJob("greedyJob", numberOfHosts, footprint);

      getApplication(cluster).execute(job);

      logMetrics(nodes);

      for (Compute node : nodes) {
         _log.info("Powering on node "+node.name());
         node.powerOn();
      }

      while(!assertWaitEquals("greedy job should fill nodes", nodes.length, job.numberOfTasks(HadoopJob.Stage.PROCESSING), _serengeti.getMaxLatency()));

      /* wait for the serengeti max latency and stats interval to expire */
      logMetrics(nodes);
      long delay = Math.max(_vCenter.getMetricsInterval(), _serengeti.getMaxLatency());
      try {
         Thread.sleep(delay);
      } catch (InterruptedException e) {}
      logMetrics(nodes);

      for (Compute node : nodes) {
         assertEquals("cpu ready value is not correct for "+node.name(), 2000, _vCenter.getRawMetric(node.name(), READY).longValue());
      }
   }

   @Test
   public void testRollingPowerOnJobDeploys() {
      final int numberOfHosts = 1;
      final int computeNodesPerHost = 8;
      String clusterName = "serengetiTest";

      Allocation hostCapacity = Allocation.zeroed();
      hostCapacity.set(ResourceType.CPU, 32000);
      hostCapacity.set(ResourceType.MEMORY, 24000);

      /* general test setup */
      setup(numberOfHosts, hostCapacity);
      _vCenter.setMetricsInterval(200);
      _serengeti.setMaxLatency(500);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      /* power on all the nodes and enable them in serengeti */
      Compute nodes[] = cluster.getComputeNodes().toArray(new Compute[0]);

      Allocation footprint = Allocation.zeroed();
      footprint.set(ResourceType.CPU, 4000);
      footprint.set(ResourceType.MEMORY, 2000);
      HadoopJob job = new EndlessTaskGreedyJob("greedyJob", numberOfHosts, footprint);

      getApplication(cluster).execute(job);

      logMetrics(nodes);

      /* rolling power on of the nodes, via target compute node num and expect not to need to explicitly enable the nodes here */
      for (int target = 1; target <= cluster.availableComputeNodes(); target++) {
         String msgid = cluster.setTargetComputeNodeNum(target);
         assertMessageResponse("waiting for target compute nodes to succeed", cluster, msgid);

         setTimeout(5000);
         assertVMsInPowerState("waiting for target compute node num ("+target+") to take effect", cluster, target, true);

         /* retry check until max latency has expired */
         logMetrics(nodes);
         int processingTasks;
         int poweredOnNodes;

         do {
            /* wait for the stats interval to expire */
            try {
               Thread.sleep(_vCenter.getMetricsInterval());
            } catch (InterruptedException e) {}

            processingTasks = job.numberOfTasks(HadoopJob.Stage.PROCESSING);
            poweredOnNodes = cluster.numberComputeNodesInPowerState(true);
         } while (timeout() >= 0 && processingTasks != poweredOnNodes);

         assertEquals("number of hadoop tasks does not match powered on nodes", poweredOnNodes, processingTasks);
      }

      logMetrics(nodes);
   }

   @Test
   public void testInvalidTarget() {
      final int numberOfHosts = 1;
      final int computeNodesPerHost = 3;
      final int target = (computeNodesPerHost * numberOfHosts) + 2;
      String clusterName = "serengetiTest";

      /* general test setup */
      setup(numberOfHosts);
      _vCenter.setMetricsInterval(200);
      _serengeti.setMaxLatency(500);

      /* create a cluster to work with */
      Master cluster = createCluster(clusterName, computeNodesPerHost);

      String msgid = cluster.setTargetComputeNodeNum(target);
      assertMessageResponse("waiting for target compute nodes to succeed", cluster, msgid);

      setTimeout(5000);
      assertVMsInPowerState("waiting for target compute node num ("+(computeNodesPerHost * numberOfHosts)+") to take effect", cluster, (computeNodesPerHost * numberOfHosts), true);
   }
}
