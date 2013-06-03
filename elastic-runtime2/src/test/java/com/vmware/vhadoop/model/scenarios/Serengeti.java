package com.vmware.vhadoop.model.scenarios;

import static com.vmware.vhadoop.vhm.model.api.ResourceType.CPU;
import static com.vmware.vhadoop.vhm.model.api.ResourceType.MEMORY;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.model.api.Allocation;
import com.vmware.vhadoop.vhm.model.os.Linux;
import com.vmware.vhadoop.vhm.model.os.Process;
import com.vmware.vhadoop.vhm.model.vcenter.Folder;
import com.vmware.vhadoop.vhm.model.vcenter.Host;
import com.vmware.vhadoop.vhm.model.vcenter.OVA;
import com.vmware.vhadoop.vhm.model.vcenter.ResourcePool;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;


public class Serengeti extends Folder
{
   public static final int UNSET = -1;

   private static Logger _log = Logger.getLogger(Serengeti.class.getName());

   /** default number of standard cpus for compute nodes */
   long defaultCpus = 2;
   /** default memory for compute nodes in Mb */
   long defaultMem = 2 * 1024;

   VirtualCenter vCenter;
   MasterTemplate masterOva;

   /**
    * Creates a "Serengeti" and adds it to the specified Orchestrator
    * @param id
    * @param orchestrator
    */
   public Serengeti(String id, VirtualCenter vCenter) {
      super(id);

      this.vCenter = vCenter;
      vCenter.add(this);
      masterOva = new MasterTemplate();
   }

   public VirtualCenter getVCenter() {
      return vCenter;
   }

   public Master createCluster(String name) {
      Allocation capacity = com.vmware.vhadoop.vhm.model.Allocation.zeroed();
      capacity.set(CPU, defaultCpus * vCenter.getCpuSpeed());
      capacity.set(MEMORY, defaultMem * 2);

      Master master = (Master) vCenter.createVM(name, capacity, masterOva);
      Folder folder = new Folder(name+"-folder");
      add(folder);
      folder.add(master);
      vCenter.add(folder);
      master.folder = folder;
      return master;
   }


   /***************** Compute Node start *****************************************************/
   class ComputeTemplate implements OVA<Compute> {
      Master master;

      ComputeTemplate(Master master) {
         this.master = master;
      }

      @Override
      public Compute create(VirtualCenter vCenter, String id, Allocation capacity) {
         Compute compute = new Compute(vCenter, master, id, capacity);
         compute.install(new Linux("Linux"));
         return compute;
      }
   }

   class Compute extends VM
   {
      Compute(VirtualCenter vCenter, Master master, String id, Allocation capacity) {
         super(vCenter, id, capacity);
         setExtraInfo("vhmInfo.elastic", "true");
         setExtraInfo("vhmInfo.masterVM.uuid", master.getClusterId());
         setExtraInfo("vhmInfo.masterVM.moid", master.getId());

         _log.info(master.clusterId+": created cluster compute node ("+id+")");
      }

      public void execute(Process process) {
         getOS().exec(process);
      }
   }
   /***************** Compute Node end *****************************************************/




   /***************** Master Node start *****************************************************/
   class MasterTemplate implements OVA<Master> {
      @Override
      public Master create(VirtualCenter vCenter, String id, Allocation capacity) {
         Master master = new Master(vCenter, id, capacity);
         master.install(new Linux("Linux"));

         return master;
      }
   }

   /**
    * This class represents the Serengeti master VM
    * @author ghicken
    *
    */
   public class Master extends VM implements EventProducer
   {
      public Folder folder;
      String clusterName;
      String clusterId;
      int computeNodesId = 0;
      List<Compute> computeNodes;
      ResourcePool computePool;
      EventConsumer eventConsumer;
      int targetComputeNodeNum = UNSET;
      ComputeTemplate computeOVA = new ComputeTemplate(this);

      public String getClusterId() {
         return clusterId;
      }

      Master(VirtualCenter vCenter, String cluster, Allocation capacity) {
         super(vCenter, cluster+"-master", capacity);
         clusterName = cluster;
         clusterId = getId();
         computeNodes = new LinkedList<Compute>();
         setExtraInfo("vhmInfo.elastic", "false");
         setExtraInfo("vhmInfo.masterVM.uuid", clusterId);
         setExtraInfo("vhmInfo.masterVM.moid", clusterId);
         setExtraInfo("vhmInfo.serengeti.uuid", Serengeti.this.getId());
         setExtraInfo("vhmInfo.jobtracker.port", "8080");
         setMinInstances(UNSET);
         setTargetComputeNodeNum(targetComputeNodeNum);
         enableAuto(false);
         _log.info(clusterId+": created cluster master ("+getId()+")");
      }


      /**
       * If we're in manual mode, this ensure that we're meeting our minimum obligation for
       * compute nodes
       */
      private void applyTarget() {
         if (eventConsumer == null) {
            return;
         }

         int target;
         if (isAuto()) {
            target = getMinInstances();
         } else {
            target = targetComputeNodeNum;
         }

         if (target == UNSET) {
            return;
         }

         /* if we're setting to manual, then generate a serengeti limit instruction */
         if (!isAuto()) {
            /* TODO: decide whether we want to support a callback mechanism */
            _log.info(clusterId+": dispatching SerengetiLimitInstruction ("+target+")");
            eventConsumer.placeEventOnQueue(new SerengetiLimitInstruction(folder.name(), SerengetiLimitInstruction.actionSetTarget, target, null));
         }
      }

      /**
       * This switches between auto and manual mode. When switching to manual mode it treats minInstances as
       * the targetComputeNodeNumber, when switching to auto it treats it as the minInstances.
       * @param enabled - true for auto, false for manual
       */
      public void enableAuto(boolean enabled) {
         setExtraInfo("vhmInfo.vhm.enable", Boolean.toString(enabled));
         applyTarget();
      }

      /**
       * Reports whether the cluster is in auto or manual mode
       */
      public boolean isAuto() {
         return Boolean.valueOf(getExtraInfo().get("vhmInfo.vhm.enable"));
      }

      public int getMinInstances() {
         int l = 0;
         String val = getExtraInfo().get("vhmInfo.min.computeNodeNum");
         if (val != null) {
            l = Integer.valueOf(val);
         }

         return l;
      }

      public void setMinInstances(int min) {
         String old = setExtraInfo("vhmInfo.min.computeNodeNum", Integer.toString(min));
         if (old != null && Integer.valueOf(old) != min) {
            applyTarget();
         }
      }

      public void setTargetComputeNodeNum(int target) {
         if (targetComputeNodeNum != target) {
            targetComputeNodeNum = target;
            applyTarget();
         }
      }

      public int getComputeNodesInPowerState(boolean power) {
         int nodes = 0;
         for (Compute compute : computeNodes) {
            if (compute.powerState() == power) {
               nodes++;
            }
         }

         return nodes;
      }

      public int availableComputeNodes() {
         return computeNodes.size();
      }

      public void createComputeNodes(int num, Host host) {
         if (computePool == null) {
            computePool = new ResourcePool(vCenter, clusterName+"-computeRP");
         }

         for (int i = 0; i < num; i++) {
            Allocation capacity = com.vmware.vhadoop.vhm.model.Allocation.zeroed();
            capacity.set(CPU, defaultCpus * vCenter.getCpuSpeed());
            capacity.set(MEMORY, defaultMem);

            Compute compute = (Compute) vCenter.createVM(clusterName+"-compute"+(computeNodesId++), capacity, computeOVA);
            compute.setExtraInfo("vhmInfo.serengeti.uuid", Serengeti.this.getId());
            /* assign it to a host */
            host.add(compute);
            /* keep it handy for future operations */
            computeNodes.add(compute);
            /* add it to the "cluster folder" and the compute node resource pool */
            folder.add(compute);
            computePool.add(compute);
            /* add this to the vApp so that we've a solid accounting for everything */
            Serengeti.this.add(compute);
         }
      }

      public void execute(Process process) {
         for (Compute node : computeNodes) {
            node.execute(process);
         }
      }

      @Override
      public void registerEventConsumer(EventConsumer vhm) {
         eventConsumer = vhm;

         /* ensure that we're meeting our minimums */
         applyTarget();
      }

      @Override
      public void start() {
         /* no-op */
      }

      @Override
      public void stop() {
         /* no-op */
      }
   }
   /***************** Master Node end *****************************************************/
}
