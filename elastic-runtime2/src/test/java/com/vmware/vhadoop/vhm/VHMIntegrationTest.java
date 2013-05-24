package com.vmware.vhadoop.vhm;

import java.util.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.TrivialClusterScaleEvent.ChannelReporter;
import com.vmware.vhadoop.vhm.TrivialScaleStrategy.TrivialClusterScaleOperation;

public class VHMIntegrationTest extends AbstractJUnitTest implements EventProducer, ClusterMapReader {
   VHM _vhm;
   StandaloneSimpleVCActions _vcActions;
   ExtraInfoToClusterMapper _strategyMapper;
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
      _strategyMapper = new ExtraInfoToClusterMapper() {
         @Override
         public String getStrategyKey(VMEventData vmd) {
            return STRATEGY_KEY;
         }

         @Override
         public Map<String, String> parseExtraInfo(VMEventData vmd) {
            return null;
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
      MultipleReaderSingleWriterClusterMapAccess.destroy();
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

   private Set<ClusterScaleCompletionEvent> waitForClusterScaleCompletionEvents(String clusterId, int numExpected, int timeoutMillis) {
      Set<ClusterScaleCompletionEvent> result = new HashSet<ClusterScaleCompletionEvent>();
      int timeoutLeft = timeoutMillis;
      while ((result.size() < numExpected) && (timeoutLeft > 0)) {
         long timeNow = System.currentTimeMillis();
         ClusterScaleCompletionEvent newEvent = waitForClusterScaleCompletionEvent(clusterId, timeoutLeft, result);
         if (newEvent != null) {
            result.add(newEvent);
         }
         timeoutLeft -= (System.currentTimeMillis() - timeNow);
      }
      return result;
   }

   private ClusterScaleCompletionEvent waitForClusterScaleCompletionEvent(String clusterId, int timeoutMillis) {
      return waitForClusterScaleCompletionEvent(clusterId, timeoutMillis, null);
   }

   private ClusterScaleCompletionEvent waitForClusterScaleCompletionEvent(String clusterId, int timeoutMillis, 
         Set<ClusterScaleCompletionEvent> thatsNotInSet) {
      ClusterScaleCompletionEvent toWaitFor = null;
      long pollTimeMillis = 10;
      long maxIterations = timeoutMillis / pollTimeMillis;
      boolean nullOrInSet = true;
      /* VHM may still be in the process of dealing with the events coming from the CSCL, so need to wait for the expected value */
      while (nullOrInSet) {
         ClusterMap clusterMap = getAndReadLockClusterMap();
         assertNotNull(clusterMap);
         toWaitFor = clusterMap.getLastClusterScaleCompletionEvent(clusterId);
         nullOrInSet = (toWaitFor == null) || ((thatsNotInSet != null) && (thatsNotInSet.contains(toWaitFor)));
         unlockClusterMap(clusterMap);
         try {
            Thread.sleep(pollTimeMillis);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         if (--maxIterations <= 0) return null;
      }
      return toWaitFor;
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
      _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(clusterId));
      /* Wait for VHM to respond, having invoked the ScaleStrategy */
      assertNotNull(waitForClusterScaleCompletionEvent(clusterId, 2000));
      
      assertEquals(clusterId, tcso.getClusterId());
      assertNotNull(tcso.getContext());
      assertNotNull(tcso.getThreadLocalCompoundStatus());
   }
   
   private class WaitResult {
      ClusterScaleCompletionEvent _testNotFinished;
      ClusterScaleCompletionEvent _testFinished;
      String _routeKeyReported;
   }
   
   @Test
   /* Two different clusters are scaled concurrently */
   public void testInvokeConcurrentScaleStrategy() {
      int numClusters = 3;
      populateSimpleClusterMap(numClusters, 4, false);    /* Blocks until CSCL has generated all events */
      assertTrue(waitForTargetClusterCount(3, 1000));
      
      Iterator<String> i = _clusterNames.iterator();
      final String clusterId1 = deriveClusterIdFromClusterName(i.next());
      final String clusterId2 = deriveClusterIdFromClusterName(i.next());
      final String clusterId3 = deriveClusterIdFromClusterName(i.next());
      
      final int cluster1delay = 3000;
      final int cluster2delay = 2000;
      final int cluster3delay = 1000;
      
      final String routeKey1 = "route1";
      final String routeKey2 = "route2";
      final String routeKey3 = "route3";
      
      /* Create ClusterScaleOperations, which control how a cluster is scaled - each operation gets a different delay to its "scaling" */
      TrivialClusterScaleOperation tcso1 = _trivialScaleStrategy.new TrivialClusterScaleOperation(cluster1delay);
      TrivialClusterScaleOperation tcso2 = _trivialScaleStrategy.new TrivialClusterScaleOperation(cluster2delay);
      TrivialClusterScaleOperation tcso3 = _trivialScaleStrategy.new TrivialClusterScaleOperation(cluster3delay);

      /* Add the test ClusterScaleOperation to the test scale strategy, which is a singleton */
      _trivialScaleStrategy.setClusterScaleOperation(clusterId1, tcso1);
      _trivialScaleStrategy.setClusterScaleOperation(clusterId2, tcso2);
      _trivialScaleStrategy.setClusterScaleOperation(clusterId3, tcso3);

      final WaitResult waitResult1 = new WaitResult();
      final WaitResult waitResult2 = new WaitResult();
      final WaitResult waitResult3 = new WaitResult();

      /* Each event has a channel it can report completion back on, along with a routeKey */
      ChannelReporter reporter1 = new ChannelReporter() {
         @Override
         public void reportBack(String routeKey) {
            waitResult1._routeKeyReported = routeKey;
         }
      };
      ChannelReporter reporter2 = new ChannelReporter() {
         @Override
         public void reportBack(String routeKey) {
            waitResult2._routeKeyReported = routeKey;
         }
      };
      ChannelReporter reporter3 = new ChannelReporter() {
         @Override
         public void reportBack(String routeKey) {
            waitResult3._routeKeyReported = routeKey;
         }
      };

      /* Simulate cluster scale events being triggered from an EventProducer */
      _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(null, null, clusterId1, routeKey1, reporter1));
      _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(null, null, clusterId2, routeKey2, reporter2));
      _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(null, null, clusterId3, routeKey3, reporter3));

      /* Three threads concurrently wait for the response from the 3 clusters and check that the response is neither early nor significantly late */
      new Thread(new Runnable(){
         @Override
         public void run() {
            waitResult1._testNotFinished = waitForClusterScaleCompletionEvent(clusterId1, cluster1delay-500);
            waitResult1._testFinished = waitForClusterScaleCompletionEvent(clusterId1, 1500);
      }}).start();

      new Thread(new Runnable(){
         @Override
         public void run() {
            waitResult2._testNotFinished = waitForClusterScaleCompletionEvent(clusterId2, cluster2delay-500);
            waitResult2._testFinished = waitForClusterScaleCompletionEvent(clusterId2, 1500);
      }}).start();

      new Thread(new Runnable(){
         @Override
         public void run() {
            waitResult3._testNotFinished = waitForClusterScaleCompletionEvent(clusterId3, cluster3delay-500);
            waitResult3._testFinished = waitForClusterScaleCompletionEvent(clusterId3, 1500);
      }}).start();
      
      /* Wait for the above threads to return the results */
      try {
         Thread.sleep(cluster1delay + 2000);
      } catch (InterruptedException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

      /* Check that the operations completed concurrently and blocked at the correct times */
      assertNull(waitResult1._testNotFinished);
      assertNotNull(waitResult1._testFinished);
      assertEquals(routeKey1, waitResult1._routeKeyReported);
      
      assertNull(waitResult2._testNotFinished);
      assertNotNull(waitResult2._testFinished);
      assertEquals(routeKey2, waitResult2._routeKeyReported);

      assertNull(waitResult3._testNotFinished);
      assertNotNull(waitResult3._testFinished);
      assertEquals(routeKey3, waitResult3._routeKeyReported);
   }

   @Test
   /* Ensure that the same cluster cannot be concurrently scaled */
   public void negativeTestConcurrentSameCluster() {
      int numClusters = 3;
      populateSimpleClusterMap(numClusters, 4, false);    /* Blocks until CSCL has generated all events */
      assertTrue(waitForTargetClusterCount(3, 1000));

      int delayMillis = 3000;
      String clusterId = deriveClusterIdFromClusterName(_clusterNames.iterator().next());
      TrivialClusterScaleOperation tcso = _trivialScaleStrategy.new TrivialClusterScaleOperation(delayMillis);
      _trivialScaleStrategy.setClusterScaleOperation(clusterId, tcso);

      try {
         _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(clusterId));
         Thread.sleep(1000);
         _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(clusterId));
         Thread.sleep(1000);
         _eventConsumer.placeEventOnQueue(new TrivialClusterScaleEvent(clusterId));
      } catch (InterruptedException e) {
         assertTrue("Unexpected interruptedException", false);
      }

      /* Expectation here is that 1st event will trigger a scale. The second one arrives and third one arrives and are both queued back up */
      /* Then, the second and third ones are processed together in a single invocation */
      
      /* This call should time out - there should only be one that's been processed so far... */
      Set<ClusterScaleCompletionEvent> results1 = waitForClusterScaleCompletionEvents(clusterId, 2, 4000);
      assertEquals(1, results1.size());

      /* The two extra events should have been picked up and should result in a second consolidated invocation. This should not time out. */
      Set<ClusterScaleCompletionEvent> results2 = waitForClusterScaleCompletionEvents(clusterId, 2, 4000);
      assertEquals(2, results2.size());
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
