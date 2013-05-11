package com.vmware.vhadoop.vhm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToScaleStrategyMapper;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.TrivialScaleStrategy.TrivialClusterScaleOperation;

public class VHMIntegrationTest extends AbstractJUnitTest implements EventProducer, ClusterMapReader {
   VHM _vhm;
   StandaloneSimpleVCActions _vcActions;
   ExtraInfoToScaleStrategyMapper _strategyMapper;
   ClusterStateChangeListenerImpl _clusterStateChangeListener;
   TrivialScaleStrategy _trivialScaleStrategy;
   
   public static final String STRATEGY_KEY = "StrategyKey";
   
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
            return STRATEGY_KEY;
         }
      };
      _trivialScaleStrategy = new TrivialScaleStrategy(STRATEGY_KEY);
      _vhm = new VHM(_vcActions, new ScaleStrategy[]{_trivialScaleStrategy}, _strategyMapper, new ThreadLocalCompoundStatus());
      _vhm.registerEventProducer(_clusterStateChangeListener);
      _vhm.registerEventProducer(this);
      _vhm.start();
   }

   @After
   public void cleanup() {
      try {
         Thread.sleep(1000);
      } catch (InterruptedException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      _vhm.stop(true);
   }
   
   /* After updating VHM via the CSCL, wait for the updates to appear in ClusterMap */
   private boolean waitForTargetClusterCount(int clusterCount, long timeoutMillis) {
      String[] clusterIds = null;
      long pollTimeMillis = 10;
      long maxIterations = timeoutMillis / pollTimeMillis;
      /* VHM may still be in the process of dealing with the events coming from the CSCL, so need to wait for the expected value */
      while ((clusterIds == null) || (clusterIds.length != clusterCount)) {
         ClusterMap clusterMap = getAndReadLockClusterMap();
         assertNotNull(clusterMap);
         clusterIds = clusterMap.getAllKnownClusterIds();
         unlockClusterMap(clusterMap);
         try {
            Thread.sleep(pollTimeMillis);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         if (--maxIterations <= 0) return false;
      }
      return true;
    }

   private boolean waitForClusterScaleCompletionEvent(String clusterId, int timeoutMillis) {
      ClusterScaleCompletionEvent toWaitFor = null;
      long pollTimeMillis = 10;
      long maxIterations = timeoutMillis / pollTimeMillis;
      /* VHM may still be in the process of dealing with the events coming from the CSCL, so need to wait for the expected value */
      while (toWaitFor == null) {
         ClusterMap clusterMap = getAndReadLockClusterMap();
         assertNotNull(clusterMap);
         toWaitFor = clusterMap.getLastClusterScaleCompletionEvent(clusterId);
         unlockClusterMap(clusterMap);
         try {
            Thread.sleep(pollTimeMillis);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         if (--maxIterations <= 0) return false;
      }
      return true;
   }

   @Test
   public void sanityTest() {
      int numClusters = 3;
      populateSimpleClusterMap(numClusters, 4, false);    /* Blocks until CSCL has generated all events */
      assertTrue(waitForTargetClusterCount(3, 1000));

      ClusterMap clusterMap = getAndReadLockClusterMap();
      assertNotNull(clusterMap);
      String[] clusterIds = clusterMap.getAllKnownClusterIds();
      unlockClusterMap(clusterMap);
      
      /* Verify that the number of clusters seen by vcActions is reflected in the ClusterMap */
      assertNotNull(clusterIds);
      assertEquals(numClusters, clusterIds.length);
   }

   @Test
   public void testInvokeScaleStrategy() {
      int numClusters = 3;
      populateSimpleClusterMap(numClusters, 4, false);    /* Blocks until CSCL has generated all events */
      assertTrue(waitForTargetClusterCount(3, 1000));
      
      String clusterId = deriveClusterIdFromClusterName(_clusterNames.iterator().next());
      /* Create a ClusterScaleOperation, which controls how a cluster is scaled */
      TrivialClusterScaleOperation tcso = _trivialScaleStrategy.new TrivialClusterScaleOperation();
      /* Add the test ClusterScaleOperation to the test scale strategy */
      _trivialScaleStrategy.setClusterScaleOperation(clusterId, tcso);

      /* Simulate a cluster scale event being triggered from an EventProducer */
      _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(null, null, clusterId));
      /* Wait for VHM to respond, having invoked the ScaleStrategy */
      assertTrue(waitForClusterScaleCompletionEvent(clusterId, 1000));
      
      assertEquals(clusterId, tcso.getClusterId());
      assertNotNull(tcso.getContext());
      assertNotNull(tcso.getThreadLocalCompoundStatus());
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
