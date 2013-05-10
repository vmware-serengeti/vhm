package com.vmware.vhadoop.vhm;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public class ClusterMapAccessTest {
   StandaloneSimpleClusterMap _clusterMap;
   MultipleReaderSingleWriterClusterMapAccess _clusterMapAccess;
   List<Thread> _liveThreads = new ArrayList<Thread>();
   
   @Before
   public void initialize() {
      _clusterMap = new StandaloneSimpleClusterMap(false);
      _clusterMapAccess = MultipleReaderSingleWriterClusterMapAccess.getClusterMapAccess(_clusterMap);
   }
   
   @After
   public void destroy() {
      MultipleReaderSingleWriterClusterMapAccess.destroy();
   }
   
   @Test
   public void DoubleLockTest() {
      ClusterMap cm = _clusterMapAccess.lockClusterMap();
      Assert.assertNotNull(cm);
      cm = _clusterMapAccess.lockClusterMap();
      Assert.assertNull(cm);
      _clusterMapAccess.unlockClusterMap(cm);
   }

   @Test
   public void DoubleUnLockTest() {
      ClusterMap cm = _clusterMapAccess.lockClusterMap();
      Assert.assertNotNull(cm);
      boolean result = _clusterMapAccess.unlockClusterMap(cm);
      Assert.assertTrue(result);
      result = _clusterMapAccess.unlockClusterMap(cm);
      Assert.assertFalse(result);
   }
   
   class TestClusterMapReader extends AbstractClusterMapReader {
      public int getNumPoweredOffVMs(long delayMillis) {
         ClusterMap cm = getAndReadLockClusterMap();
         try {
            Thread.sleep(delayMillis);
         } catch (InterruptedException e) {}
         Set<String> vmIds = cm.listComputeVMsForClusterAndPowerState("myCluster", false);
         unlockClusterMap(cm);
         return (vmIds == null) ? 0 : vmIds.size();
      }
   }
   
   private AtomicInteger startReaders(final List<TestClusterMapReader> readers, final long readDelayMillis, final int assertResult) {
      final AtomicInteger numStarted = new AtomicInteger();
      
      for (final TestClusterMapReader reader : readers) {
         Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
               numStarted.incrementAndGet();
               System.out.println(System.currentTimeMillis()+": Reader trying to read");
               Assert.assertEquals(assertResult, reader.getNumPoweredOffVMs(readDelayMillis));
               System.out.println(System.currentTimeMillis()+": Reader done reading");
            }});
         _liveThreads.add(t);
         t.start();
      }
      
      return numStarted;
   }
   
   private AtomicInteger startWriter(final long writeDelayMillis) {
      final AtomicInteger writing = new AtomicInteger();
      
      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            _clusterMapAccess.runCodeInWriteLock(new Callable<Object>() {
               @Override
               public Object call() throws Exception {
                  writing.incrementAndGet();
                  System.out.println(System.currentTimeMillis()+": Writer started writing. New Readers should be blocked for "+writeDelayMillis+"ms");
                  Thread.sleep(writeDelayMillis);
                  _clusterMap.addVMToMap("myVm3", "myCluster", "myHost", false);
                  return false;
               }
            });
         }});
      _liveThreads.add(t);
      t.start();
      return writing;
   }

   private void blockUntilCompletion() {
      int aliveCount = _liveThreads.size();
      while (aliveCount > 0) {
         aliveCount = 0;
         try {
            for (Thread t : _liveThreads) {
               if (t.isAlive()) aliveCount++;
            }
            Thread.sleep(10);
         } catch (InterruptedException e) {}
      }
   }
   
   private void blockUntilIntegerEquals(AtomicInteger value, int expected) {
      while (value.get() != expected) {
         try {
            Thread.sleep(10);
         } catch (InterruptedException e) {}
      }
   }

   @Test
   public void testCorrectBehavior() {
      ClusterMapReader parent = new AbstractClusterMapReader(_clusterMapAccess, null) {};

      /* Create a clustermap with 2 powered off VMs */
      _clusterMap.addVMToMap("myVm1", "myCluster", "myHost", false);
      _clusterMap.addVMToMap("myVm2", "myCluster", "myHost", false);
 
      /* Create 3 cluster map readers */
      final List<TestClusterMapReader> readers = new ArrayList<TestClusterMapReader>();
      for (int i=0; i<3; i++) {
         TestClusterMapReader reader = new TestClusterMapReader();
         readers.add(reader);
         reader.initialize(parent);
      }

      long readDelayMillis = 200;
      long writeDelayMillis = 1000;
      
      /* Start the 3 readers in 3 threads. Once numStarted == 3, the reader threads have started and will then delay for the requested time */
      AtomicInteger numStarted = startReaders(readers, readDelayMillis, 2);
      blockUntilIntegerEquals(numStarted, 3);
      System.out.println(System.currentTimeMillis()+": Readers started reading. Writer should be blocked for "+readDelayMillis+"ms");

      /* The writer thread is started while the readers are still reading, so should be prevented from writing until the readers have completed */
      AtomicInteger writing = startWriter(writeDelayMillis);
      blockUntilIntegerEquals(writing, 1);
      
      /* The line above blocks until the writer starts writing, at which point we start 3 new reader threads which all assert that they see the updated value */
      startReaders(readers, 0, 3);
      
      /* Wait until all threads have completed */
      blockUntilCompletion();
   }
}
