package com.vmware.vhadoop.vhm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToScaleStrategyMapper;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;

public class VHMIntegrationTest extends AbstractJUnitTest implements EventProducer, ClusterMapReader {
   VHM _vhm;
   StandaloneSimpleVCActions _vcActions;
   ExtraInfoToScaleStrategyMapper _strategyMapper;
   ClusterStateChangeListenerImpl _clusterStateChangeListener;
   
   /* EventProducer fields */
   EventConsumer _eventConsumer;
   
   /* ClusterMapReader fields */
   ClusterMapAccess _clusterMapAccess;

   @Override
   public void initialize(final ClusterMapReader parent) {
      if (parent instanceof AbstractClusterMapReader) {
         _clusterMapAccess = ((AbstractClusterMapReader)parent)._clusterMapAccess;
      } else {
         throw new RuntimeException("Unrecognized ClusterMapReader implementation");
      }
   }

   @Override
   void processNewEventData(VMEventData eventData) {
      _vcActions.fakeWaitForUpdatesData("", eventData);
   }
   
   @Before
   public void initialize() {
      _vcActions = new StandaloneSimpleVCActions();
      _clusterStateChangeListener = new ClusterStateChangeListenerImpl(_vcActions, "myFolder");
      _strategyMapper = new ExtraInfoToScaleStrategyMapper() {
         @Override
         public String getStrategyKey(VMEventData vmd) {
            return ManualScaleStrategy.MANUAL_SCALE_STRATEGY_KEY;
         }
      };
      _vhm = new VHM(_vcActions, new ScaleStrategy[]{new TrivialScaleStrategy("fooKey")}, _strategyMapper, null);
      _vhm.registerEventProducer(_clusterStateChangeListener);
      _vhm.registerEventProducer(this);
      _vhm.start();
   }

   @After
   public void cleanup() {
      _vhm.stop(true);
   }
   
   @Test
   public void sanityTest() {
      int numClusters = 3;
      
      populateSimpleClusterMap(numClusters, 4, false);    /* Blocks until CSCL has generated all events */
      String[] clusterIds = null;
      int maxIterations = 100;
      
      /* VHM may still be in the process of dealing with the events coming from the CSCL, so need to wait for the expected value */
      while ((clusterIds == null) || (clusterIds.length != 3)) {
         ClusterMap clusterMap = getAndReadLockClusterMap();
         assertNotNull(clusterMap);
         clusterIds = clusterMap.getAllKnownClusterIds();
         unlockClusterMap(clusterMap);
         try {
            Thread.sleep(10);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         if (--maxIterations <= 0) break;
      }
      
      assertNotNull(clusterIds);
      assertEquals(numClusters, clusterIds.length);
   }

   @Override
   public void registerEventConsumer(EventConsumer eventConsumer) {
      _eventConsumer = eventConsumer;
   }

   @Override
   public void start() {
      /* No-op */
   }

   @Override
   public void stop() {
      /* No-op */
   }

   @Override
   /* Gets a read lock on ClusterMap - call unlock when done */
   public ClusterMap getAndReadLockClusterMap() {
      return _clusterMapAccess.lockClusterMap();
   }

   @Override
   public void unlockClusterMap(final ClusterMap clusterMap) {
      _clusterMapAccess.unlockClusterMap(clusterMap);
   }
}
