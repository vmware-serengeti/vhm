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

import static com.vmware.vhadoop.vhm.model.api.ResourceType.CPU;
import static com.vmware.vhadoop.vhm.model.api.ResourceType.MEMORY;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.model.api.Allocation;
import com.vmware.vhadoop.vhm.model.vcenter.Folder;
import com.vmware.vhadoop.vhm.model.vcenter.OVA;
import com.vmware.vhadoop.vhm.model.vcenter.VM;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.rabbit.ModelRabbitAdaptor.RabbitConnectionCallback;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;


public class Serengeti extends Folder implements EventProducer
{
   public static final int UNSET = -1;
   final static String COMPUTE_SUBDOMAIN = "compute";
   final static String ROUTEKEY_SEPARATOR = ":";
   public static final String UNKNOWN_HOSTNAME_FOR_COMPUTE_NODE = "unknown hostname for compute node";
   public static final String COMPUTE_NODE_IN_UNDETERMINED_STATE = "compute node was neither enabled or disabled";
   public static final String COMPUTE_NODE_ALREADY_IN_TARGET_STATE = "compute node was already in the target state";

   /** The frequency with which we want to check the state of the world, unprompted. milliseconds */
   long maxLatency = 5000;

   private static Logger _log = Logger.getLogger(Serengeti.class.getName());

   /** default number of standard cpus for compute nodes */
   public static final long defaultCpus = 2;
   /** default memory for compute nodes in Mb */
   public static final long defaultMem = 2 * 1024;

   VirtualCenter vCenter;

   Map<String,Master> clusters = new HashMap<String,Master>();
   Map<String,Folder> folders = new HashMap<String,Folder>();

   /** This is a record of whether VHM has asked us, as an event producer, to stop */
   boolean _stopped = false;
   EventProducerStartStopCallback _callback;
   EventConsumer eventConsumer;

   /**
    * Creates a "Serengeti" and adds it to the specified Orchestrator
    * @param id
    * @param orchestrator
    */
   public Serengeti(String id, VirtualCenter vCenter) {
      super(id);

      this.vCenter = vCenter;
      vCenter.add(this);
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

   public Master createCluster(String name, Master.Template template) {
      Allocation capacity = com.vmware.vhadoop.vhm.model.Allocation.zeroed();
      capacity.set(CPU, defaultCpus * vCenter.getCpuSpeed());
      capacity.set(MEMORY, defaultMem * 2);

      Master master = (Master) vCenter.createVM(name, capacity, template, this);
      Folder folder = new Folder(name);
      add(folder);
      folder.add(master);
      vCenter.add(folder);

      clusters.put(master.getId(), master);
      folders.put(master.getId(), folder);
      return master;
   }

   static String constructHostnameForCompute(Master master, String computeId) {
      return computeId+"."+COMPUTE_SUBDOMAIN+"."+master.clusterName;
   }

   static String getComputeIdFromHostname(String hostname) {
      int index = hostname.indexOf("."+COMPUTE_SUBDOMAIN+".");
      if (index == -1) {
         return null;
      }

      return hostname.substring(0, index);
   }

   /**
    * Generates a SerengetiLimitInstruction for delivery to the specified cluster.
    * @param clusterId the target cluster
    * @param id the message id
    * @param action the action to take
    * @param targetComputeNodeNum target number of nodes
    * @return true if instruction queued, false otherwise
    */
   public boolean generateLimitInstruction(String clusterId, String id, String action, int targetComputeNodeNum) {
      if (eventConsumer == null) {
         return false;
      }

      Folder folder = getFolder(clusterId);
      if (folder == null) {
         _log.severe(name()+": expected to have a folder associated with cluster "+clusterId+", unable to send limit instruction");
         return false;
      }

      eventConsumer.placeEventOnQueue(new SerengetiLimitInstruction(folder.name(), action, targetComputeNodeNum, new RabbitConnectionCallback(packRouteKey(id, clusterId), this)));
      return true;
   }

   public static VHMJsonReturnMessage unpackRawPayload(byte[] json) {
      Gson gson = new Gson();
      return gson.fromJson(new String(json), VHMJsonReturnMessage.class);
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

      master.deliverMessage(parts[0], msg);
   }

   public Folder getFolder(String clusterId) {
      return folders.get(clusterId);
   }


   /***************** Serengeti event producer methods *****************************************************
    * Serengeti model is an event producer solely so that we can put events directly onto the VHM queue and
    * receive responses.
    */
   @Override
   public void start(EventProducerStartStopCallback callback) {
      _callback = callback;
      if (_callback != null) {
         _callback.notifyStarted(this);
      }
      _stopped = false;
   }

   /**
    * Implements EventConsumer.stop()
    */
   @Override
   public void stop() {
      if (_callback != null && _stopped == false) {
         _callback.notifyStopped(this);
      }

      _stopped = true;
   }


   @Override
   public boolean isStopped() {
      return _stopped;
   }

   @Override
   public void registerEventConsumer(EventConsumer vhm) {
      eventConsumer = vhm;

      /* ensure that we're meeting our minimums for all clusters */
      for (Master master : clusters.values()) {
         master.applyTarget();
      }
   }
   /***************** Serengeti event producer methods - end *****************************************************/




   static public abstract class Template<T extends VM> implements OVA<T> {
      /**
       * Creates a compute VM from the specified template. The variable data should be a Master VM from the corresponding
       * Master template.
       *
       * This method is called by create cluster and in turn calls:
       * instantiate
       * configure(Master)
       * specialize(Master,Serengeti)
       */
      @Override
      final public T create(VirtualCenter vCenter, String id, Allocation capacity, Object serengeti) {
         T t = instantiate(vCenter, id, capacity, (Serengeti)serengeti);
         configure(t, (Serengeti)serengeti);
         specialize(t, (Serengeti)serengeti);

         return t;
      }

      /**
       * Creates an instance of the VM described by the template
       * @param vCenter
       * @param id
       * @param capacity
       * @param serengeti
       * @return
       */
      protected abstract T instantiate(VirtualCenter vCenter, String id, Allocation capacity, Serengeti serengeti);

      protected void configure(T t, Serengeti serengeti) {
      }

      protected void specialize(T t, Serengeti serengeti) {
      }
   }
}
