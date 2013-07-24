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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.model.api.Allocation;
import com.vmware.vhadoop.vhm.model.scenarios.BasicScenario;
import com.vmware.vhadoop.vhm.model.scenarios.Compute;
import com.vmware.vhadoop.vhm.model.scenarios.Master;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti;
import com.vmware.vhadoop.vhm.model.vcenter.Host;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;


abstract public class ModelTestBase<T extends Serengeti, M extends Master, J> extends AbstractClusterMapReader implements EventProducer {

   /** Set this to "true" to disable the test timeouts */
   public final static String DISABLE_TIMEOUT = "disable.timeout";

   protected Logger _log;
   protected VHM _vhm;
   protected VirtualCenter _vCenter;
   protected T _serengeti;

   BootstrapMain _bootstrap;
   EventConsumer _consumer;

   /** This is a record of whether VHM has asked us, as an event producer, to stop */
   boolean _stopped = false;
   EventProducerStartStopCallback _callback;

   long startTime;
   /** default timeout is two decision cycles plus warm up/cool down */
   long timeout = (2 * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME + TEST_COOLDOWN_TIME;

   static int TEST_WARM_UP_TIME = 20000;
   static int TEST_COOLDOWN_TIME = 10000;
   static int LIMIT_CYCLE_TIME = 5000;

   public ModelTestBase(Logger logger) {
      _log = logger;
   }

   public ModelTestBase() {
      _log = Logger.getLogger(this.getClass().getName());
   }


   @After
   @Before
   public void resetSingletons() {
      MultipleReaderSingleWriterClusterMapAccess.destroy();
   }

   VHM init() {
      _bootstrap = new ModelController(null, null, _serengeti);
      return _bootstrap.initVHM(new ThreadLocalCompoundStatus());
   }

   protected void startVHM() {
      _vhm = init();
      assertTrue(_vhm.registerEventProducer(this));
      _vhm.start();
   }

   /**
    * Sets the timeout and resets the start time so we can set timeouts for portions of tests
    * @param millis
    */
   protected void setTimeout(long millis) {
      timeout = millis;
      startTime = 0;
   }

   protected long timeout() {
      if (startTime == 0) {
         startTime = System.currentTimeMillis();
         return timeout;
      } else {
         boolean disableTimeout = Boolean.valueOf(System.getProperty(DISABLE_TIMEOUT, "false"));
         if (disableTimeout) {
            /** return an hour, every time we're asked */
            return 60 * 60 * 1000;
         } else {
            return startTime - System.currentTimeMillis() + timeout;
         }
      }
   }

   @Override
   public void registerEventConsumer(EventConsumer vhm) {
      _consumer = vhm;
   }

   @Override
   public void start(EventProducerStartStopCallback callback) {
      _callback = callback;
      if (_callback != null) {
         _callback.notifyStarted(this);
      }
      _stopped = false;
   }

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

   /**
    * Will create a single host for the cluster to deploy on if setup's not been called with a different number.
    * Creates a symmetrical deployment of compute nodes on the available hosts
    * @param clusterName
    * @param computeNodesPerHost
    * @return
    */
   protected M createCluster(String clusterName, int computeNodesPerHost) {
      int numberOfComputeNodes = 0;

      if (_serengeti == null) {
         setup(1);
      }

      /* create a cluster to work with */
      @SuppressWarnings("unchecked")
      M cluster = (M)_serengeti.createCluster(clusterName, getMasterTemplate());
      String clusterId = cluster.getClusterId();

      @SuppressWarnings("unchecked")
      List<Host> hosts = (List<Host>) _vCenter.get(Host.class);
      for (Host host : hosts) {
         if (cluster.getHost() == null) {
            /* master VMs need a host too */
            host.add(cluster);
         } else {
            cluster.createComputeNodes(computeNodesPerHost, host);
            numberOfComputeNodes+= computeNodesPerHost;
         }

         host.powerOn();
      }

      /* power on the master node */
      cluster.powerOn();

      assertScaleStrategySet("wait for scale strategy to be determined", clusterId);

      /* wait for VHM to register the VMs */
      assertClusterMapVMs("register VMs in cluster map", cluster, numberOfComputeNodes);

      /* dump the cluster map so we can inspect the state in the log */
      _vhm.dumpClusterMap(Level.INFO);

      return cluster;
   }

   protected abstract Master.Template getMasterTemplate();

   /**
    * This provides easy access to the application hosted in the cluster. This will most likely be
    * obtained by using the OperatingSystem.connect call to get the governing application process
    * for the cluster.
    * @param master
    * @return
    */
   protected abstract J getApplication(M master);

   /**
    * Sub-classes MUST over-ride this method to create a serengeti of the desired type
    * @param numberOfHosts
    * @return
    */
   protected abstract T createSerengeti(String name, VirtualCenter vCenter);

   protected T setup(int numberOfHosts, Allocation hostCapacity) {
      /* perform the basic test setup that ModelTestBase depends on */
      if (hostCapacity == null) {
         _vCenter = BasicScenario.getVCenter(numberOfHosts + 1, 0);
      } else {
         _vCenter = BasicScenario.getVCenter(numberOfHosts + 1, hostCapacity);
      }

      _serengeti = createSerengeti(getClass().getName()+"-vApp", _vCenter);
      _serengeti.setMaxLatency(LIMIT_CYCLE_TIME);


      /* set this to something that relates to the stats sample interval so we've got some
       * coupling with how fast we're expecting stats to change */
      _vCenter.setMetricsInterval(LIMIT_CYCLE_TIME /2);

      /* start the system */
      startVHM();

      /* register Serengeti as an event producer */
      assertTrue(_vhm.registerEventProducer(_serengeti));

      return _serengeti;
   }

   protected T setup(int numberOfHosts) {
      return setup(numberOfHosts, null);
   }

   protected Set<Host> getComputeNodeHosts(Master cluster) {
      Set<Host> hosts = new HashSet<Host>();
      Set<Compute> nodes = cluster.getComputeNodes();
      for (Compute node : nodes) {
         hosts.add(node.getHost());
      }

      return hosts;
   }

   protected void waitForMetricInterval() {
      try {
         Thread.sleep(_vCenter.getMetricsInterval());
      } catch (InterruptedException e) {}
   }


   /**
    * Clean up from this run in preparation for the next. Dumps cluster map for reference in case of
    * test failure
    */
   @After
   public void cleanup() {
      if (_vhm != null) {
         _vhm.stop(true);
         while (!_vhm.isStopped()) {
            try {
               Thread.sleep(200);
            } catch (InterruptedException e) {}
         }

         _vhm.dumpClusterMap(Level.INFO);
         _vhm = null;
      }

      if (_vCenter != null) {
         @SuppressWarnings("unchecked")
         List<Host> hosts = (List<Host>) _vCenter.get(Host.class);
         for (Host host : hosts) {
            host.powerOff();
         }

         _vCenter = null;
      }

      _serengeti = null;
   }

   /**
    * Asserts that precisely the specified number of VMs are in the specified power state, as reported via VC
    * @param msg
    * @param master
    * @param number
    * @param power
    * @param timeout
    */
   public void assertActualVMsInPowerState(String msg, Master master, int number, boolean power, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;
      _log.info(msg+" - waiting for "+number+" VMs to show as powered "+(power?"on":"off")+" in cluster "+master.getClusterId()+", timeout "+(timeout/1000)+"s");
      int current;
      do {
         long timestamp = _vCenter.getConfigurationTimestamp();
         current = master.numberComputeNodesInPowerState(power);

         if (current != number) {
            _log.info(msg+" saw "+current+" in target power state. Waiting.");
            _vCenter.waitForConfigurationUpdate(timestamp, timeout());
         }
      } while (current != number && System.currentTimeMillis() < deadline);

      assertEquals(msg+" - incorrect number of VMs powered "+(power ? "on" : "off")+" in cluster "+master.getClusterId(), number, master.numberComputeNodesInPowerState(power));
      _log.info(msg+" - "+number+" VMs powered "+(power?"on":"off")+" in cluster "+master.getClusterId());
   }

   /**
    * Provides a roll up of the multiple wait/assert functions for checking VM power states
    * @param msg
    * @param master
    * @param number
    * @param power
    */
   public void assertVMsInPowerState(String msg, Master master, int number, boolean power) {
      assertActualVMsInPowerState(msg, master, number, power);
      assertClusterMapVMsInPowerState(msg, master.getClusterId(), number, power);
   }

   /**
    * This waits for the actual VM states as reported by vCenter, NOT from the cluster map
    * @param msg
    * @param master
    * @param number
    * @param power
    */
   public void assertActualVMsInPowerState(String msg, Master master, int number, boolean power) {
      assertActualVMsInPowerState(msg, master, number, power, timeout());
   }

   /**
    * This asserts that there are precisely the specified number of compute nodes in the given cluster, irrespective of
    * power state
    * @param msg
    * @param master
    * @param number
    */
   public void assertClusterMapVMs(String msg, Master master, int number) {
      assertClusterMapVMs(msg, master, number, timeout());
   }

   /**
    * This asserts that there are precisely the specified number of compute nodes in the given cluster, irrespective of
    * power state
    * @param msg
    * @param master
    * @param number
    */
   public void assertClusterMapVMs(String msg, Master master, int number, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;
      boolean firstTime = true;

      _log.info(msg+" - waiting for "+number+" VMs to show in cluster map for cluster"+master.getClusterId()+", timeout "+(timeout/1000)+"s");
      Set<String> vms;
      do {
         if (!firstTime) {
            try {
               Thread.sleep(500);
            } catch (InterruptedException e) {}
            firstTime = false;
         }

         ClusterMap map = getAndReadLockClusterMap();
         /* we really care about number of VMs in the cluster but we know that they're starting powered off at this point */
         vms = map.listComputeVMsForCluster(master.getClusterId());
         unlockClusterMap(map);
      } while (
            (vms == null && number != 0) ||
            (vms != null && vms.size() != number) &&
            System.currentTimeMillis() < deadline);


      assertEquals(msg+" - incorrect number of VMs show in cluster map for cluster"+master.getClusterId() , number, vms != null ? vms.size() : 0);
      _log.info(msg+" - "+number+" VMs show in cluster map for cluster"+master.getClusterId());
   }

   /**
    * Asserts that precisely the specified number of VMs are in the specified power state, as reported via ClusterMap
    * @param msg
    * @param clusterId
    * @param number
    * @param power
    * @param timeout
    */
   public void assertClusterMapVMsInPowerState(String msg, String clusterId, int number, boolean power, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;
      boolean firstTime = true;

      _log.info(msg+" - waiting for "+number+" VMs to show as powered "+(power?"on":"off")+" in cluster map "+clusterId+", timeout "+(timeout/1000));
      Set<String> vms;
      do {
         if (!firstTime) {
            try {
               Thread.sleep(500);
            } catch (InterruptedException e) {}
            firstTime = false;
         }

         ClusterMap map = getAndReadLockClusterMap();
         /* we really care about number of VMs in the cluster but we know that they're starting powered off at this point */
         vms = map.listComputeVMsForClusterAndPowerState(clusterId, power);
         unlockClusterMap(map);
      } while (
            ((vms == null && number != 0) ||
            (vms != null && vms.size() != number)) &&
            System.currentTimeMillis() < deadline);

      assertEquals(msg+" - incorrect number of VMs show as powered "+(power ? "on" : "off")+" in cluster map for cluster"+clusterId , number, vms != null ? vms.size() : 0);
      _log.info(msg+" - "+number+" VMs show as powered "+(power?"on":"off")+" in cluster map for cluster"+clusterId);
   }

   /**
    * This waits and asserts states of the VMs in ClusterMap, NOT the actual state in vCenter.
    * @param msg
    * @param clusterId
    * @param number
    * @param power
    */
   public void assertClusterMapVMsInPowerState(String msg, String clusterId, int number, boolean power) {
      assertClusterMapVMsInPowerState(msg, clusterId, number, power, timeout());
   }

   /**
    * This inspects the cluster map to ensure that a scale strategy has been set, any strategy. This is an initialization check.
    * @param clusterId
    * @param number
    * @param timeout
    */
   public void assertScaleStrategySet(String msg, String clusterId, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;
      boolean firstTime = true;

      _log.info("Waiting for scale strategy to be set in cluster map for "+clusterId+", timeout "+(timeout/1000));
      String strategy = null;
      do {
         if (!firstTime) {
            try {
               Thread.sleep(500);
            } catch (InterruptedException e) {}
            firstTime = false;
         }

         ClusterMap map = getAndReadLockClusterMap();
         strategy = map.getScaleStrategyKey(clusterId);
         unlockClusterMap(map);
      } while (strategy == null && System.currentTimeMillis() < deadline);

      assertNotNull(msg+" - scale strategy wasn't registered for cluster "+clusterId, strategy);
      _log.info("scale strategy "+strategy+" registered in cluster map for cluster"+clusterId);
   }

   public void assertScaleStrategySet(String msg, String clusterId) {
      assertScaleStrategySet(msg, clusterId, timeout());
   }


   /**
    * This waits for the specified message to be delivered to the cluster master via the rabbit queue.
    * If asserts on message internals need to be done then the message should be extracted after this
    * returns via Master.waitForResponse. This will wait for completion messages.
    *
    * @param msg a descriptive message of what we're waiting for
    * @param cluster the cluster we're operating on
    * @param id the interaction id we want a reply for
    * @param timeout
    */
   public void assertMessageResponse(String msg, Master cluster, String id, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;

      _log.info(msg+" - waiting for completion response from "+cluster.getClusterId()+" with id "+id+", timeout "+(timeout/1000));
      VHMJsonReturnMessage response;
      do {
         response = cluster.waitForResponse(id, timeout);
      } while ((response == null || !response.finished) && System.currentTimeMillis() < deadline);

      assertNotNull(msg+" - expected response from "+cluster.getClusterId()+" with id "+id, response);
      assertEquals(msg+" - expected 100% complete response from "+cluster.getClusterId()+" with id "+id, 100, response.progress);

      _log.info(msg+" - received completion response from "+cluster.getClusterId()+" for id "+id+", status: "+(response.succeed ? "successful" : "failed"));
   }

   public void assertMessageResponse(String msg, Master cluster, String id) {
      assertMessageResponse(msg, cluster, id, timeout());
   }

   public boolean assertWaitEquals(String msg, Object expected, Object value, long sleep) {
      if (expected.equals(value)) {
         _log.info(msg+" - expected value "+expected.toString()+" matched");
         return true;
      }

      long timeout = timeout();
      if (timeout > 0) {
         try {
            long millis = Math.min(sleep, timeout);
            _log.info(msg+" - expected ("+expected.toString()+") saw ("+value.toString()+"), sleeping for "+millis+"ms");
            Thread.sleep(millis);
         } catch (InterruptedException e) {}
      }

      /* there's still time to try again */
      if (timeout() > 0) {
         return false;
      }

      /* we're out of time so one last chance */
      assertEquals(msg+" - failed to match expected values", expected, value);

      /* they matched at the last opportunity */
      _log.info(msg+" - expected value "+expected.toString()+" matched");
      return true;
   }
}
