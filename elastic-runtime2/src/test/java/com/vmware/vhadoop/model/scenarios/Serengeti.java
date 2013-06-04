package com.vmware.vhadoop.model.scenarios;

import static com.vmware.vhadoop.vhm.model.api.ResourceType.CPU;
import static com.vmware.vhadoop.vhm.model.api.ResourceType.MEMORY;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.model.api.Allocation;
import com.vmware.vhadoop.vhm.model.api.Workload;
import com.vmware.vhadoop.vhm.model.os.Linux;
import com.vmware.vhadoop.vhm.model.os.Process;
import com.vmware.vhadoop.vhm.model.vcenter.Folder;
import com.vmware.vhadoop.vhm.model.vcenter.Host;
import com.vmware.vhadoop.vhm.model.vcenter.OVA;
import com.vmware.vhadoop.vhm.model.vcenter.ResourcePool;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.model.workloads.HadoopJob;


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
      Folder folder = new Folder(name);
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
      Master master;

      Compute(VirtualCenter vCenter, Master master, String id, Allocation capacity) {
         super(vCenter, id, capacity);
         this.master = master;
         setExtraInfo("vhmInfo.elastic", "true");
         setExtraInfo("vhmInfo.masterVM.uuid", master.getClusterId());
         setExtraInfo("vhmInfo.masterVM.moid", master.getId());

         _log.info(master.clusterId+": created cluster compute node ("+id+")");
      }

      public void execute(Process process) {
         _log.info(name()+": executing process "+process.name());
         getOS().exec(process);
      }

      /**
       * We over-ride remove so that we can inform the master that a task is done
       */
      @Override
      public Allocation remove(Workload workload) {
         if (workload instanceof HadoopJob.Task) {
            this.master.reportEndOfTask((Process)workload);
         }

         return super.remove(workload);
      }

      @Override
      public void powerOn() {
         super.powerOn();
         _log.info(name()+": powered on, hosted by "+getHost());
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
      private static final long REFRESH = 30*1000;

      public Folder folder;
      String clusterName;
      String clusterId;
      int computeNodesId = 0;
      Set<Compute> computeNodes;
      ResourcePool computePool;
      EventConsumer eventConsumer;
      int targetComputeNodeNum = UNSET;
      ComputeTemplate computeOVA = new ComputeTemplate(this);
      Set<HadoopJob> jobs = new HashSet<HadoopJob>();
      Map<Compute,Process> tasks = new HashMap<Compute,Process>();

      public String getClusterId() {
         return clusterId;
      }

      /* TODO: serengeti master should run a Hadoop job which virtualizes the compute VMs available and asks for resource based
       * on both running and queued jobs. HadoopJobs should be run by that Hadoop job on any powered on compute nodes.
       */
      Master(VirtualCenter vCenter, String cluster, Allocation capacity) {
         super(vCenter, cluster+"-master", capacity);
         clusterName = cluster;
         clusterId = getId();
         computeNodes = new HashSet<Compute>();
         setExtraInfo("vhmInfo.masterVM.uuid", clusterId);
         setExtraInfo("vhmInfo.masterVM.moid", clusterId); /* I don't know if uuid and moid have to be the same, but it works if they are */
         setExtraInfo("vhmInfo.elastic", "false");
         setExtraInfo("vhmInfo.jobtracker.port", "8080");

         /* serengeti.uuid is the folder id for the cluster. This must contain at least one VM from the cluster or we can't
          * correlate limit instructions with clusters. If not set here it will be discovered based on the cluster name passed
          * by the limit instruction
          */
         setExtraInfo("vhmInfo.serengeti.uuid", "");

         /* these two are necessary for manual mode to work, even though they aren't applicable */
         setExtraInfo("vhmInfo.vhm.enable", "false");
         setExtraInfo("vhmInfo.min.computeNodeNum", "0");

         setTargetComputeNodeNum(targetComputeNodeNum);

         _log.info(clusterId+": created cluster master ("+getId()+")");
      }


      /**
       * ensure that we're meeting our obligation for compute nodes
       */
      protected void applyTarget() {
         if (eventConsumer == null) {
            return;
         }

         if (targetComputeNodeNum == UNSET) {
            return;
         }

         /* TODO: decide whether we want to support a callback mechanism */
         _log.info(clusterId+": dispatching SerengetiLimitInstruction ("+targetComputeNodeNum+")");
         eventConsumer.placeEventOnQueue(new SerengetiLimitInstruction(folder.name(), SerengetiLimitInstruction.actionSetTarget, targetComputeNodeNum, null));
      }

      public void setTargetComputeNodeNum(int target) {
         if (targetComputeNodeNum != target) {
            targetComputeNodeNum = target;
            applyTarget();
         }
      }

      public int numberComputeNodesInPowerState(boolean power) {
         int nodes = 0;
         for (Compute compute : computeNodes) {
            if (compute.powerState() == power) {
               nodes++;
            }
         }

         return nodes;
      }

      public Set<Compute> getComputeNodesInPowerState(boolean power) {
         Set<Compute> compute = new HashSet<Compute>();
         for (Compute node : computeNodes) {
            if (node.powerState() == power) {
               compute.add(node);
            }
         }

         return compute;
      }

      public Set<Compute> getComputeNodes() {
         return Collections.unmodifiableSet(computeNodes);
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
            compute.setExtraInfo("vhmInfo.serengeti.uuid", folder.name());
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

      public synchronized void execute(HadoopJob job) {
         jobs.add(job);
         reviseResourceUsage();
      }

      /**
       * Callback for tasks to report completion
       * @param task
       */
      void reportEndOfTask(Process task) {
         reviseResourceUsage();
      }

      @Override
      protected synchronized Allocation getDesiredAllocation() {
         /* re-evaluate whether we can run more tasks */
         _log.info(name()+": re-evaluating job distribution on compute nodes");

         /* have any tasks completed? */
         for (Compute node : computeNodes) {
            if (!node.powerState()) {
               continue;
            }

            Process task = tasks.get(node);
            if (task != null) {
               if (task.alive()) {
                  /* think about looking at the tasks progress */
               } else {
                  tasks.remove(node);

                  /* TODO: think about where adding the tasks final progress to the job total makes sense */
                  task = null;
               }
            }

            if (task == null) {
               /* schedule a new task */
               for (HadoopJob job : jobs) {
                  task = job.getTask();
                  if (task != null) {
                     node.execute(task);
                     tasks.put(node,  task);
                     break;
                  }
               }
            }
         }

         /* we don't want to allocate anything on our own behalf based on tasks */
         return super.getDesiredAllocation();
      }
   }

   /***************** Master Node end *****************************************************/
}
