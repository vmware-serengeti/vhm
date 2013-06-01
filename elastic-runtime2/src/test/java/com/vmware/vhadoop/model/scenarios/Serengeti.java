package com.vmware.vhadoop.model.scenarios;

import java.util.LinkedList;
import java.util.List;

import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.model.Host;
import com.vmware.vhadoop.model.Orchestrator;
import com.vmware.vhadoop.model.ResourceContainer;
import com.vmware.vhadoop.model.ResourcePool;
import com.vmware.vhadoop.model.VM;
import com.vmware.vhadoop.model.Workload;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;

public class Serengeti extends ResourceContainer
{
   /** default number of standard cpus for compute nodes */
   long defaultCpus = 2;
   /** default memory for compute nodes in Mb */
   long defaultMem = 2 * 1024;

   /**
    * Creates a "Serengeti" and adds it to the specified Orchestrator
    * @param id
    * @param orchestrator
    */
   public Serengeti(String id, Orchestrator orchestrator) {
      super(id, orchestrator);
      orchestrator.add(this);
   }

   public Master createCluster(String name) {
      Master master = new Master(name+"-master", name, orchestrator);
      add(master);
      return master;
   }

   class Compute extends VM
   {
      Compute(String id, Master master, Orchestrator orchestrator) {
         super(id, defaultCpus * orchestrator.getCpuSpeed(), defaultMem, orchestrator);
         setExtraInfo("vhmInfo.elastic", "true");
         setExtraInfo("vhmInfo.masterVM.uuid", master.clusterId);
         setExtraInfo("vhmInfo.masterVM.moid", master.getId());
      }
   }

   /**
    * This class represents the Serengeti master VM
    * @author ghicken
    *
    */
   public class Master extends VM implements EventProducer
   {
      String clusterName;
      String clusterId;
      int computeNodesId = 0;
      List<Compute> computeNodes;
      ResourcePool computePool;
      EventConsumer eventConsumer;
      
      public String getClusterId() {
         return clusterId;
      }

      Master(String id, String cluster, Orchestrator orchestrator) {
         super(id, 2 * orchestrator.getCpuSpeed(), 4 * 1024, orchestrator);
         clusterName = cluster;
         clusterId = id;
         computeNodes = new LinkedList<Compute>();
         setExtraInfo("vhmInfo.elastic", "false");
         setExtraInfo("vhmInfo.masterVM.uuid", clusterId);
         setExtraInfo("vhmInfo.masterVM.moid", id);
         setExtraInfo("vhmInfo.serengeti.uuid", Serengeti.this.getId());
         setExtraInfo("vhmInfo.jobtracker.port", "8080");
         setMinInstances(UNLIMITED);
         enableAuto(false);
         computePool = new ResourcePool(clusterName, orchestrator);
         this.add(computePool);
      }

      /**
       * If we're in manual mode, this ensure that we're meeting our minimum obligation for
       * compute nodes
       */
      protected void applyMinInstances() {
         /* if we're setting to manual, then generate a serengeti limit instruction */
         if (!isAuto() && eventConsumer != null) {
            int min = Integer.valueOf(getExtraInfo().get("vhmInfo.min.computeNodeNum"));
            /* TODO: decide whether we want to support a callback mechanism */
            eventConsumer.placeEventOnQueue(new SerengetiLimitInstruction(clusterName, SerengetiLimitInstruction.actionSetTarget, min, null));
         }
      }

      /**
       * This switches between auto and manual mode. When switching to manual mode it treats minInstances as
       * the targetComputeNodeNumber, when switching to auto it treats it as the minInstances.
       * @param enabled - true for auto, false for manual
       */
      public void enableAuto(boolean enabled) {
         setExtraInfo("vhmInfo.vhm.enable", Boolean.toString(enabled));         
         applyMinInstances();
      }

      /**
       * Reports whether the cluster is in auto or manual mode
       */
      public boolean isAuto() {
         return Boolean.valueOf(getExtraInfo().get("vhmInfo.vhm.enable"));
      }

      public void setMinInstances(long min) {
         setExtraInfo("vhmInfo.min.computeNodeNum", Long.toString(min));
         applyMinInstances();
      }

      public long getMinInstances() {
         long l = UNLIMITED;
         String val = getExtraInfo().get("vhmInfo.min.computeNodeNum");
         if (val != null) {
            l = Long.valueOf(val);
         }

         return l;
      }

      public int getCurrentInstances() {
         int poweredOn = 0;
         for (Compute compute : computeNodes) {
            if (compute.getPowerState()) {
               poweredOn++;
            }
         }

         return poweredOn;
      }

      public void createComputeNodes(int num, Host host) {
         for (int i = 0; i < num; i++) {
            Compute compute = new Compute(clusterName+"-compute"+(computeNodesId++), this, orchestrator);
            compute.setExtraInfo("vhmInfo.serengeti.uuid", Serengeti.this.getId());
            /* assign it to a host */
            host.add(compute);
            /* keep it handy for future operations */
            computeNodes.add(compute);
            /* add it to the "cluster folder", the compute node resource pool in this case */
            computePool.add(compute);
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

      @Override
      public void registerEventConsumer(EventConsumer vhm) {
         eventConsumer = vhm;

         /* ensure that we're meeting our minimums */
         applyMinInstances();
      }

      @Override
      public void start() {
         /* noop */
      }

      @Override
      public void stop() {
         /* noop */
      }
   }
}
