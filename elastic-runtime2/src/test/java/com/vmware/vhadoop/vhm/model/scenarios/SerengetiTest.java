package com.vmware.vhadoop.vhm.model.scenarios;

import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.vmware.vhadoop.vhm.model.Allocation;
import com.vmware.vhadoop.vhm.model.api.ResourceType;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.Compute;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.Master;
import com.vmware.vhadoop.vhm.model.vcenter.Host;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.model.workloads.EndlessTaskGreedyJob;
import com.vmware.vhadoop.vhm.model.workloads.HadoopJob;

public class SerengetiTest
{
   private static Logger _log = Logger.getLogger(SerengetiTest.class.getName());
   VirtualCenter vCenter;
   Serengeti serengeti;

   private void logMetrics(Compute nodes[]) {
      for (Compute node : nodes) {
         if (!node.powerState()) {
            continue;
         }

         long metrics[] = vCenter.getMetrics(node.name());
         StringBuilder sb = new StringBuilder(node.name());
         for (int i = 0; i < 3; i++) {
            sb.append("\t").append(VirtualCenter.getMetricNames()[i]).append(": ").append(+metrics[i]);
         }

         _log.info(sb.toString());
      }
   }

   @Before
   public void setup() {
      vCenter = new VirtualCenter("SerengetiTest-vcenter");
      serengeti = new Serengeti("SerengetiTest-vApp", vCenter);

   }

   @Test
   public void test() {
      Master cluster = serengeti.createCluster("cluster1");

      Host hosts[] = new Host[2];
      Compute nodes[] = new Compute[8];

      Allocation hostCapacity = Allocation.zeroed();
      hostCapacity.set(ResourceType.CPU, 8000);
      hostCapacity.set(ResourceType.MEMORY, 24000);

      for (int i = 0; i < hosts.length; i++) {
         int num = nodes.length/hosts.length;
         hosts[i] = vCenter.createHost("host"+i, hostCapacity);
         Compute c[] = cluster.createComputeNodes(num, hosts[i]);
         System.arraycopy(c, 0, nodes, i*num, c.length);
      }

      for (int i = 0; i < hosts.length; i++) {
         hosts[i].powerOn();
      }

      Allocation footprint = Allocation.zeroed();
      footprint.set(ResourceType.CPU, 4000);
      footprint.set(ResourceType.MEMORY, 2000);
      HadoopJob job = new EndlessTaskGreedyJob("greedyJob", hosts.length, footprint);

      cluster.execute(job);

      logMetrics(nodes);

      for (Compute node : nodes) {
         _log.info("Powering on node "+node.name());
         node.powerOn();
         cluster.enable(node.getHostname());

         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {}

         logMetrics(nodes);
      }
   }
}
