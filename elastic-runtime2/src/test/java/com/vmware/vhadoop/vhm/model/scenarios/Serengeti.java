package com.vmware.vhadoop.vhm.model.scenarios;

import static com.vmware.vhadoop.vhm.model.api.ResourceType.CPU;
import static com.vmware.vhadoop.vhm.model.api.ResourceType.MEMORY;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.EventProducer.EventProducerStoppingCallback;
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
import com.vmware.vhadoop.vhm.rabbit.ModelRabbitAdaptor.RabbitConnectionCallback;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;


public class Serengeti extends Folder
{
   public static final int UNSET = -1;
   final static String COMPUTE_SUBDOMAIN = "compute";
   final static String ROUTEKEY_SEPARATOR = ":";

   /** The frequency with which we want to check the state of the world, unprompted. milliseconds */
   long maxLatency = 5000;

   private static Logger _log = Logger.getLogger(Serengeti.class.getName());

   /** default number of standard cpus for compute nodes */
   long defaultCpus = 2;
   /** default memory for compute nodes in Mb */
   long defaultMem = 2 * 1024;

   VirtualCenter vCenter;
   MasterTemplate masterOva;

   Map<String,Master> clusters = new HashMap<String,Master>();

   /** This is a record of whether VHM has asked us, as an event producer, to stop */
   boolean _stopped = false;
   EventProducerStoppingCallback _callback;

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

   /**
    * Specifies how frequently the cluster Master should wake up and inspect the state of the world.
    * It may be prompted to take action by events more frequently than this limit
    * @param millis latency in milliseconds
    */
   public void setMaxLatency(long millis) {
      maxLatency = millis;
   }

   public long getMaxLatency() {
      return maxLatency;
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

      clusters.put(master.getId(), master);
      return master;
   }

   static String constructHostnameForCompute(Master master, String computeId) {
      return computeId+"."+COMPUTE_SUBDOMAIN+"."+master.clusterName;
   }

   public static VHMJsonReturnMessage unpackRawPayload(byte[] json) {
      Gson gson = new Gson();
      return gson.fromJson(new String(json), VHMJsonReturnMessage.class);
   }

   static String getComputeIdFromHostname(String hostname) {
      int index = hostname.indexOf("."+COMPUTE_SUBDOMAIN+".");
      if (index == -1) {
         return null;
      }

      return hostname.substring(0, index);
   }

   static String packRouteKey(String msgId, String clusterId) {
      return msgId + ROUTEKEY_SEPARATOR + clusterId;
   }

   static String[] unpackRouteKey(String routeKey) {
      if (routeKey == null) {
         return null;
      }

      int index = routeKey.indexOf(ROUTEKEY_SEPARATOR);
      if (index == -1) {
         return null;
      }

      String id = routeKey.substring(0, index);
      String remainder = null;
      if (index != routeKey.length() - 1) {
         remainder = routeKey.substring(index+1);
      }

      return new String[] {id, remainder};
   }


   /**
    * Allows VHM to send 'RabbitMQ' messages to this serengeti detailing the results of actions
    * The message is passed along to the master in question
    * @param data
    */
   public void deliverMessage(byte[] data) {
      deliverMessage(null, data);
   }

   /**
    * Allows VHM to send 'RabbitMQ' messages to this serengeti detailing the results of actions
    * The message is passed along to the master in question
    * @param routeKey
    * @param data
    */
   public void deliverMessage(String routeKey, byte[] data) {
      _log.info(name()+" received message on route '"+routeKey+"': "+new String(data));

      VHMJsonReturnMessage msg = unpackRawPayload(data);
      String parts[] = unpackRouteKey(routeKey);
      if (parts == null || parts.length < 2) {
         _log.severe(name()+": received message reply without a destination cluster: "+routeKey);
         return;
      }

      Master master = clusters.get(parts[1]);
      if (master == null) {
         _log.severe(name()+": received message with unknown destination cluster: "+routeKey);
         return;
      }

      master.deliverMessage(routeKey, msg);
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
         compute.install(new Linux("Linux-"+id));
         compute.setHostname(Serengeti.constructHostnameForCompute(master, id));

         return compute;
      }
   }

   public class Compute extends VM
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
         _log.info(name()+": powered on, hosted by "+getHost().name());
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
      int msgId = 0;
      final Set<Compute> computeNodes = new HashSet<Compute>();
      final Map<String,Compute> enabled = new HashMap<String,Compute>();
      final Map<String,Compute> disabled = new HashMap<String,Compute>();
      ResourcePool computePool;
      EventConsumer eventConsumer;
      int targetComputeNodeNum = UNSET;
      final ComputeTemplate computeOVA = new ComputeTemplate(this);
      Set<HadoopJob> jobs = new HashSet<HadoopJob>();
      final Map<Compute,Process> tasks = new HashMap<Compute,Process>();

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
       * Allows VHM to send 'RabbitMQ' messages to this master detailing the results of actions
       * @param data
       */
      public void deliverMessage(String msgId, VHMJsonReturnMessage msg) {
         _log.info(name()+": received message, id: "+msgId+
                                       ", finished: "+msg.finished+
                                       ", succeeded: "+msg.succeed+
                                       ", progress: "+msg.progress+
                                       ", error_code: "+msg.error_code+
                                       ", error_msg: "+msg.error_msg+
                                       ", progress_msg: "+msg.progress_msg);

         /* TODO: track inflight messages somehow */
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
         eventConsumer.placeEventOnQueue(new SerengetiLimitInstruction(folder.name(), SerengetiLimitInstruction.actionSetTarget, targetComputeNodeNum, new RabbitConnectionCallback(packRouteKey(Integer.toString(msgId++), clusterId), Serengeti.this)));
      }

      public void setTargetComputeNodeNum(int target) {
         if (targetComputeNodeNum != target) {
            targetComputeNodeNum = target;
            applyTarget();
         }
      }

      public int numberComputeNodesInPowerState(boolean power) {
         int nodes = 0;
         long timestamp = vCenter.getConfigurationTimestamp();
         long timestamp2 = 0;

         while (timestamp != timestamp2) {
            synchronized(computeNodes) {
               for (Compute compute : computeNodes) {
                  if (compute.powerState() == power) {
                     nodes++;
                  }
               }
            }
            /* check to see if state changed under our accounting */
            timestamp2 = vCenter.getConfigurationTimestamp();
         }

         return nodes;
      }

      public int numberComputeNodesInState(boolean enabled) {
         return this.enabled.size();
      }

      public Set<Compute> getComputeNodesInPowerState(boolean power) {
         Set<Compute> compute = new HashSet<Compute>();
         long timestamp = vCenter.getConfigurationTimestamp();
         long timestamp2 = 0;

         while (timestamp != timestamp2) {
            synchronized(computeNodes) {
               for (Compute node : computeNodes) {
                  if (node.powerState() == power) {
                     compute.add(node);
                  }
               }
            }
            /* check to see if state changed under our accounting */
            timestamp2 = vCenter.getConfigurationTimestamp();
         }

         return compute;
      }

      public Set<Compute> getComputeNodes() {
         return Collections.unmodifiableSet(computeNodes);
      }

      public int availableComputeNodes() {
         return computeNodes.size();
      }

      public Compute[] createComputeNodes(int num, Host host) {
         if (computePool == null) {
            computePool = new ResourcePool(vCenter, clusterName+"-computeRP");
         }

         Compute nodes[] = new Compute[num];
         for (int i = 0; i < num; i++) {
            Allocation capacity = com.vmware.vhadoop.vhm.model.Allocation.zeroed();
            capacity.set(CPU, defaultCpus * vCenter.getCpuSpeed());
            capacity.set(MEMORY, defaultMem);

            Compute compute = (Compute) vCenter.createVM(clusterName+"-compute"+(computeNodesId++), capacity, computeOVA);
            nodes[i] = compute;

            compute.setExtraInfo("vhmInfo.serengeti.uuid", folder.name());
            /* assign it to a host */
            host.add(compute);
            /* keep it handy for future operations */
            synchronized (computeNodes) {
               /* we expose this external via various accessor methods that iterate over it */
               computeNodes.add(compute);
            }

            /* add it to the "cluster folder" and the compute node resource pool */
            folder.add(compute);
            computePool.add(compute);
            /* mark it as disabled to hadoop */
            disabled.put(compute.getId(), compute);
            /* add this to the vApp so that we've a solid accounting for everything */
            Serengeti.this.add(compute);
         }

         return nodes;
      }

      @Override
      public void registerEventConsumer(EventConsumer vhm) {
         eventConsumer = vhm;

         /* ensure that we're meeting our minimums */
         applyTarget();
      }


      /**
       * Makes the node available for running tasks
       * @param hostname
       * @return null on success, error detail otherwise
       */
      public String enable(String hostname) {
         String id = getComputeIdFromHostname(hostname);
         if (id == null) {
            return "unknown hostname for task tracker";
         }

         Compute node = disabled.remove(id);
         if (node == null) {
            if (enabled.containsKey(id)) {
               return "task tracker was already enabled";
            }

            return "task tracker was neither enabled or disabled!";
         }

         _log.info(name()+" enabling compute node "+id);

         enabled.put(id, node);

         /* revise our job distribution if needed */
         scheduleNewTask(node);

         /* return null on success */
         return null;
      }

      /**
       * Stops the task running on the specified compute node
       * and disables it from further consideration
       * @param hostname
       * @return null on success, error detail otherwise
       */
      public String disable(String hostname) {
         String id = getComputeIdFromHostname(hostname);
         if (id == null) {
            return "unknown hostname for task tracker";
         }

         Compute node = enabled.remove(id);
         if (node == null) {
            if (disabled.containsKey(id)) {
               return "task tracker was already disabled";
            }

            return "task tracker was neither enabled or disabled!";
         }

         Compute old = disabled.put(id, node);
         if (old == null) {
            Process task = tasks.get(node);
            if (task != null) {
               /* TODO: implement external termination of Processes */
            }

            _log.info(name()+" disabled compute node "+id);
         }

         disabled.put(id, node);

         /* return null on success */
         return null;
      }


      public synchronized void execute(HadoopJob job) {
         _log.info(name()+" received job for dispatch, "+job.name());

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
         for (Compute node : enabled.values()) {
            if (!node.powerState()) {
               /* TODO: do we want to blacklist this node as it shows as enabled and is not */
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
               scheduleNewTask(node);
            }
         }

         /* we don't want to allocate anything on our own behalf based on tasks */
         Allocation desired = super.getDesiredAllocation();
         if (desired.getDuration() > maxLatency) {
            desired.setDuration(maxLatency);
         }

         return desired;
      }

      /**
       * Sees if we can schedule a task on the given node
       * @param node
       * @return true if a task was started, false otherwise
       */
      private boolean scheduleNewTask(Compute node) {
         try {
            _log.info(name()+": prompted to look at shceduling a task on node "+node.name());
            if (node.powerState()) {
               for (HadoopJob job : jobs) {
                  if (job.queueSize() > 0) {
                     Process task = job.getTask();
                     if (task != null) {
                        node.execute(task);
                        tasks.put(node, task);
                        return true;
                     }
                  }
               }
            }
         } catch (IllegalStateException e) {
            /* it's possible that we're trying to run a job on a recently powered off node */
            _log.warning(e.getMessage());
         }

         return false;
      }

      @Override
      public void start(EventProducerStoppingCallback callback) {
         _callback = callback;
         _stopped = false;
      }

      /**
       * Implements EventConsumer.stop()
       */
      @Override
      public void stop() {
         if (_callback != null && _stopped == false) {
            _callback.notifyStopping(this, false);
         }

         _stopped = true;
      }


      @Override
      public boolean isStopped() {
         return _stopped;
      }
   }

   /***************** Master Node end *****************************************************/
}
