package com.vmware.vhadoop.scenarios;

import java.util.LinkedList;
import java.util.List;

import com.vmware.vhadoop.model.Host;
import com.vmware.vhadoop.model.Orchestrator;
import com.vmware.vhadoop.model.ResourceContainer;
import com.vmware.vhadoop.model.VM;
import com.vmware.vhadoop.model.Workload;

public class Serengeti extends ResourceContainer
{
   /** default number of standard cpus for compute nodes */
   long defaultCpus = 2;
   /** default memory for compute nodes in Mb */
   long defaultMem = 2 * 1024;

   public Serengeti(String id, Orchestrator orchestrator) {
      super(id, orchestrator);
   }

   public Master createCluster(String name) {
      Master master = new Master(name+"-master", name, orchestrator);
      add(master);
      return master;
   }

   private class Node extends VM {
      Node(String id, long cpu, long mem, String master, Orchestrator orchestrator) {
         super(id, cpu, mem, orchestrator);
         setExtraInfo("vhmInfo.masterVM.uuid", master);
         setExtraInfo("vhmInfo.masterVM.moid", master);
      }
   }

   class Compute extends Node
   {
      Compute(String id, String master, Orchestrator orchestrator) {
         super(id, defaultCpus * orchestrator.getCpuSpeed(), defaultMem, master, orchestrator);
         setExtraInfo("vhmInfo.elastic", "true");
      }
   }

   public class Master extends Node
   {
      String clusterId;
      int computeNodesId = 0;
      List<Compute> computeNodes;

      Master(String id, String cluster, Orchestrator orchestrator) {
         super(id, 2 * orchestrator.getCpuSpeed(), 4 * 1024, id, orchestrator);
         clusterId = cluster;
         computeNodes = new LinkedList<Compute>();
         setExtraInfo("vhmInfo.serengeti.uuid", clusterId);
         setMinInstances(UNLIMITED);
         enableAuto(false);
      }

      public void enableAuto(boolean enabled) {
         setExtraInfo("vhmInfo.vhm.enable", Boolean.toString(enabled));
      }

      public void setMinInstances(long min) {
         setExtraInfo("vhmInfo.min.computeNodeNum", Long.toString(min));
      }

      public void createComputeNodes(int num, Host host) {
         for (int i = 0; i < num; i++) {
            Compute compute = new Compute(clusterId+"-compute"+(computeNodesId++), getId(), orchestrator);
            compute.setExtraInfo("vhmInfo.serengeti.uuid", clusterId);
            host.add(compute);
            computeNodes.add(compute);
            /* add this to the vApp so that we've a solid accounting for everything */
            Serengeti.this.add(compute);
         }
      }

      @Override
      public void execute(Workload workload) {
         for (Compute node : computeNodes) {
            node.execute(workload);
         }
      }
   }
}
