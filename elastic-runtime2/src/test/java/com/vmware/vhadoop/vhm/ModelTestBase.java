package com.vmware.vhadoop.vhm;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.model.scenarios.Serengeti;
import com.vmware.vhadoop.model.scenarios.Serengeti.Master;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.vc.VcVlsi;

abstract public class ModelTestBase extends AbstractClusterMapReader implements EventProducer {
   /* front load the cost of the springframework initialization */
   static final VcVlsi _VCVLSI = new VcVlsi();

   Logger _log;
   VHM _vhm;
   VirtualCenter _vCenter;
   Serengeti _serengeti;

   BootstrapMain _bootstrap;
   EventConsumer _consumer;

   long startTime;
   /** default timeout is two decision cycles plus warm up/cool down */
   long timeout = (2 * LIMIT_CYCLE_TIME) + TEST_WARM_UP_TIME + TEST_COOLDOWN_TIME;

   static int TEST_WARM_UP_TIME = 20000;
   static int TEST_COOLDOWN_TIME = 10000;
   static int LIMIT_CYCLE_TIME = 1000000;

   public ModelTestBase(Logger logger) throws IOException, ClassNotFoundException {
      /* force this to load so that the springframework binding is done before we invoke tests */
      ClassLoader.getSystemClassLoader().loadClass("com.vmware.vhadoop.vhm.vc.VcVlsi");
      _log = logger;
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
      _vhm.registerEventProducer(this);
      _vhm.start();
   }

   protected void setTimeout(long millis) {
      timeout = millis;
   }

   protected long timeout() {
      if (startTime == 0) {
         startTime = System.currentTimeMillis();
         return timeout;
      } else {
         return startTime - System.currentTimeMillis() + timeout;
      }
   }

   @Override
   public void registerEventConsumer(EventConsumer vhm) {
      _consumer = vhm;
   }

   @Override
   public void start(EventProducerStoppingCallback callback) {
      /* noop */
   }

   @Override
   public void stop() {
      /* noop */
   }

   @Override
   public boolean isStopped() {
      // TODO Auto-generated method stub
      return false;
   }

   public void assertActualVMsInPowerState(String msg, Master master, int number, boolean power, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;

      _log.info("Waiting for VMs to power "+(power?"on":"off")+" in cluster "+master.getClusterId()+", timeout "+(timeout/1000));
      while (master.getComputeNodesInPowerState(power) < number && System.currentTimeMillis() < deadline) {
         _vCenter.waitForConfigurationUpdate(timeout());
      }

      assertEquals(msg+" - not enough powered "+(power ? "on" : "off")+" in cluster "+master.getClusterId(), number, master.getComputeNodesInPowerState(power));
      _log.info("VMs powered "+(power?"on":"off")+" in cluster "+master.getClusterId());
   }

   /**
    * This inspects the cluster map for VMs in the specified state
    * @param clusterId
    * @param number
    * @param power
    * @param timeout
    */
   public void assertClusterMapVMsInPowerState(String msg, String clusterId, int number, boolean power, long timeout) {
      long deadline = System.currentTimeMillis() + timeout;

      _log.info("Waiting for VMs to power "+(power?"on":"off")+" in cluster map "+clusterId+", timeout "+(timeout/1000));
      Set<String> vms;
      do {
         try {
            Thread.sleep(500);
         } catch (InterruptedException e) {}
         ClusterMap map = getAndReadLockClusterMap();
         /* we really care about number of VMs in the cluster but we know that they're starting powered off at this point */
         vms = map.listComputeVMsForClusterAndPowerState(clusterId, power);
         unlockClusterMap(map);
      } while ((vms == null || vms.size() < number) && System.currentTimeMillis() < deadline);

      assertEquals(msg+" - not enough powered "+(power ? "on" : "off")+" in cluster "+clusterId , number, vms != null ? vms.size() : 0);
      _log.info("VMs powered "+(power?"on":"off")+" in cluster map for cluster"+clusterId);
   }
}
