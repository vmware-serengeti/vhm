package com.vmware.vhadoop.vhm.model.scenarios;

import static com.vmware.vhadoop.vhm.model.api.ResourceType.CPU;
import static com.vmware.vhadoop.vhm.model.api.ResourceType.MEMORY;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.model.api.Allocation;
import com.vmware.vhadoop.vhm.model.os.Linux;
import com.vmware.vhadoop.vhm.model.vcenter.Folder;
import com.vmware.vhadoop.vhm.model.vcenter.Host;
import com.vmware.vhadoop.vhm.model.vcenter.ResourcePool;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;

/**
 * This class represents the master VM of a cluster
 * @author ghicken
 *
 */
public class Master extends VM
{
   public static class Template extends Serengeti.Template<Master> {
      @Override
      protected Master instantiate(VirtualCenter vCenter, String id, Allocation capacity, Serengeti serengeti) {
         return new Master(vCenter, id, capacity, serengeti);
      }

      @Override
      protected void configure(Master master, Serengeti serengeti) {
         master.install(new Linux("Linux"));
         master.setHostname("master."+master.clusterName);
      }

      /**
       * Specializes the Serengeti cluster master for a given deployment
       * @param master
       * @param data
       */
      @Override
      protected void specialize(Master master, Serengeti serengeti) {
      }
   }

   private static Logger _log = Logger.getLogger(Master.class.getName());

   Serengeti serengeti;
   String clusterName;
   String clusterId;
   int computeNodesId = 0;
   int msgId = 0;
   final Set<Compute> computeNodes = new HashSet<Compute>();
   final Map<String,Compute> enabled = new HashMap<String,Compute>();
   final Map<String,Compute> disabled = new HashMap<String,Compute>();
   ResourcePool computePool;
   int targetComputeNodeNum = Serengeti.UNSET;
   Compute.Template computeOVA = new Compute.Template(this);
   final Map<String,VHMJsonReturnMessage> messages = new HashMap<String,VHMJsonReturnMessage>();

   public String getClusterId() {
      return clusterId;
   }

   protected Master(VirtualCenter vCenter, String cluster, Allocation capacity, Serengeti serengeti) {
      super(vCenter, cluster+"-master-0", capacity);
      this.serengeti = serengeti;

      clusterName = cluster;
      clusterId = getId();
      setExtraInfo("vhmInfo.masterVM.uuid", clusterId);
      setExtraInfo("vhmInfo.masterVM.moid", clusterId); /* I don't know if uuid and moid have to be the same, but it works if they are */
      setExtraInfo("vhmInfo.elastic", "false");

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

      synchronized(messages) {
         messages.put(msgId, msg);
         messages.notifyAll();
      }
   }

   /**
    * This waits for a response message from VHM with the given id. Currently this only notifies
    * when the completion message arrives, but logs the arrival of progress updates.
    *
    * If the wait times out then the most recent response matching the ID will be returned, or null
    * if none have been seen.
    *
    * @param id
    * @param timeout
    * @return
    */
   public VHMJsonReturnMessage waitForResponse(String id, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;
      long remaining = timeout;
      int progress = -1;

      synchronized(messages) {
         VHMJsonReturnMessage response = messages.get(id);
         try {
            while ((response == null || !response.finished) && remaining > 0) {
               messages.wait(remaining);
               remaining = deadline - System.currentTimeMillis();
               response = messages.get(id);
               if (response != null && response.progress != progress) {
                  _log.info(name()+": received update for interaction "+id+", progress: "+progress);
                  progress = response.progress;
               }
            }
         } catch (InterruptedException e) {}

         return response;
      }
   }


   /**
    * ensure that we're meeting our obligation for compute nodes
    * @return the msgId for the message under which we will see replies, or null if the message could not be queued
    */
   protected String applyTarget() {
      String id = null;

      if (targetComputeNodeNum == Serengeti.UNSET) {
         return id;
      }

      _log.info(clusterId+": dispatching SerengetiLimitInstruction ("+targetComputeNodeNum+")");
      String currentId = Integer.toString(msgId);
      if (serengeti.generateLimitInstruction(clusterId, currentId, SerengetiLimitInstruction.actionSetTarget, targetComputeNodeNum)) {
         msgId++;
         id = currentId;
      }

      return id;
   }

   /**
    * Sets the target compute node number for the cluster
    * @param target the number of nodes we want
    * @return the id of the interaction for retrieving responses, null if the command could not be dispatched
    */
   public String setTargetComputeNodeNum(int target) {
      if (targetComputeNodeNum != target) {
         targetComputeNodeNum = target;
         return applyTarget();
      }

      return null;
   }

   public int numberComputeNodesInPowerState(boolean power) {
      int nodes = 0;
      long timestamp = 0;
      long timestamp2 = 0;

      do {
         nodes=0;
         timestamp = this.vCenter.getConfigurationTimestamp();
         _log.fine("Comparing timestamps pre-lock: "+timestamp+" != "+timestamp2);
         synchronized(computeNodes) {
            _log.info("Entered synchronized block");
            for (Compute compute : computeNodes) {
               if (compute.powerState() == power) {
                  _log.finer("Incrementing nodes to "+nodes);
                  nodes++;
               }
            }
         }
         _log.info("Exited synchronized block");

         /* check to see if state changed under our accounting */
         timestamp2 = vCenter.getConfigurationTimestamp();
      } while (timestamp != timestamp2);

      _log.fine("Done comparing timestamps - nodes = "+nodes);
      return nodes;
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

   public int numberComputeNodesInState(boolean enabled) {
      return this.enabled.size();
   }

   public synchronized Collection<Compute> getComputeNodesInState(boolean enabled) {
      if (enabled) {
         return new HashSet<Compute>(this.enabled.values());
      }

      return new HashSet<Compute>(this.disabled.values());
   }

   public Set<Compute> getComputeNodes() {
      synchronized(computeNodes) {
         return new HashSet<Compute>(computeNodes);
      }
   }

   public int availableComputeNodes() {
      return computeNodes.size();
   }

   public void setComputeNodeTemplate(Compute.Template template) {
      this.computeOVA = template;
   }

   public Compute[] createComputeNodes(int num, Host host) {
      Folder folder = serengeti.getFolder(clusterId);
      if (folder == null) {
         _log.severe(name()+": unable to get folder for cluster "+clusterId+", unable to create compute nodes");
         return null;
      }

      if (computePool == null) {
         computePool = new ResourcePool(vCenter, clusterName+"-computeRP");
      }

      Compute nodes[] = new Compute[num];
      for (int i = 0; i < num; i++) {
         Allocation capacity = com.vmware.vhadoop.vhm.model.Allocation.zeroed();
         capacity.set(CPU, Serengeti.defaultCpus * vCenter.getCpuSpeed());
         capacity.set(MEMORY, Serengeti.defaultMem);

         Compute compute = (Compute) vCenter.createVM(clusterName+"-"+host.name()+"-compute"+(computeNodesId++), capacity, computeOVA, serengeti);
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
         synchronized(this) {
            disabled.put(compute.getId(), compute);
         }

         /* add this to the vApp so that we've a solid accounting for everything */
         serengeti.add(compute);
      }

      return nodes;
   }


   @Override
   protected Allocation getDesiredAllocation() {
      /* we don't want to allocate anything on our own behalf based on tasks */
      Allocation desired = super.getDesiredAllocation();
      if (desired != null) {
         if (desired.getDuration() > serengeti.getMaxLatency()) {
            desired.setDuration(serengeti.getMaxLatency());
         }
      }

      return desired;
   }

   /**
    * Makes the node available for running tasks
    * @param hostname
    * @return null on success, error detail otherwise
    */
   public synchronized String enable(String hostname) {
      String id = Serengeti.getComputeIdFromHostname(hostname);
      if (id == null) {
         return Serengeti.UNKNOWN_HOSTNAME_FOR_COMPUTE_NODE;
      }

      Compute node = disabled.remove(id);
      if (node == null) {
         if (enabled.containsKey(id)) {
            return Serengeti.COMPUTE_NODE_ALREADY_IN_TARGET_STATE;
         }

         return Serengeti.COMPUTE_NODE_IN_UNDETERMINED_STATE;
      }

      _log.info(name()+" enabling compute node "+id);

      enabled.put(id, node);

      /* revise our job distribution if needed */
      reviseResourceUsage();

      /* return null on success */
      return null;
   }

   /**
    * Stops the task running on the specified compute node
    * and disables it from further consideration
    * @param hostname
    * @return null on success, error detail otherwise
    */
   public synchronized String disable(String hostname) {
      String id = Serengeti.getComputeIdFromHostname(hostname);
      if (id == null) {
         return Serengeti.UNKNOWN_HOSTNAME_FOR_COMPUTE_NODE;
      }

      Compute node = enabled.remove(id);
      if (node == null) {
         if (disabled.containsKey(id)) {
            return Serengeti.COMPUTE_NODE_ALREADY_IN_TARGET_STATE;
         }

         return Serengeti.COMPUTE_NODE_IN_UNDETERMINED_STATE;
      }

      Compute old = disabled.put(id, node);
      if (old == null) {
         _log.info(name()+" disabled compute node "+id);
      }

      disabled.put(id, node);

      /* return null on success */
      return null;
   }
}
