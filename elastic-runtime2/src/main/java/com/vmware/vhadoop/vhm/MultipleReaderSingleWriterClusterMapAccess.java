package com.vmware.vhadoop.vhm;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader.ClusterMapAccess;

public class MultipleReaderSingleWriterClusterMapAccess implements ClusterMapAccess {
   private Set<Thread> _readerThreads = Collections.synchronizedSet(new HashSet<Thread>());
   private Object _clusterMapWriteLock = new Object();
   private ClusterMap _clusterMap;
   private static MultipleReaderSingleWriterClusterMapAccess _singleton;

   private static final Logger _log = Logger.getLogger(MultipleReaderSingleWriterClusterMapAccess.class.getName());

   public static MultipleReaderSingleWriterClusterMapAccess getClusterMapAccess(ClusterMap clusterMap) {
      if (_singleton == null) {
         _singleton = new MultipleReaderSingleWriterClusterMapAccess(clusterMap);
      }
      return _singleton;
   }
   
   /* for testing only - ugh */
   protected static void destroy() {
      _singleton = null;
   }
   
   private MultipleReaderSingleWriterClusterMapAccess(ClusterMap clusterMap) {
      _clusterMap = clusterMap;
   }
   
   @Override
   public ClusterMap lockClusterMap() {
      Thread currentThread = Thread.currentThread();
      if (!_readerThreads.contains(currentThread)) {
         /* All readers will be blocked during a ClusterMap write and while ClusterMap waits for the reader count to go to zero */
         synchronized(_clusterMapWriteLock) {
            _readerThreads.add(currentThread);
         }
         return _clusterMap;
      } else {
         _log.severe("Attempt to double-lock ClusterMap!");
         return null;
      }
   }

   @Override
   public boolean unlockClusterMap(ClusterMap clusterMap) {
      Thread currentThread = Thread.currentThread();
      if (_readerThreads.contains(currentThread)) {
         _readerThreads.remove(currentThread);
         return true;
      } else {
         _log.severe("Attempt to double-unlock ClusterMap!");
         return false;
      }
   }
   
   Object runCodeInWriteLock(Callable<Object> callable) {
      synchronized(_clusterMapWriteLock) {
         try {
            /* Wait for the readers to stop reading. New readers will block on the write lock */
            long readerTimeout = 1000;
            long pollSleep = 10;
            long killCntr = readerTimeout / pollSleep;
            while (_readerThreads.size() > 0) {
               try {
                  Thread.sleep(pollSleep);
                  if (--killCntr < 0) {
                     _log.severe("Reader lock left open. Whacking to 0!");
                     _readerThreads.clear();
                     break;
                  }
               } catch (InterruptedException e) {
                  _log.warning("Unexpected interruption to sleep in writeLock");
               }
            }
            return callable.call();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }
}
