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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader.ClusterMapAccess;

public class MultipleReaderSingleWriterClusterMapAccess implements ClusterMapAccess {
   private final Set<Thread> _readerThreads = Collections.synchronizedSet(new HashSet<Thread>());
   private final Object _clusterMapWriteLock = new Object();
   private final ClusterMap _clusterMap;
   private static MultipleReaderSingleWriterClusterMapAccess _singleton;

   private static final Logger _log = Logger.getLogger(MultipleReaderSingleWriterClusterMapAccess.class.getName());

   /* THREADING: Only accessed by single thread, so no need for synchronization */
   static MultipleReaderSingleWriterClusterMapAccess getClusterMapAccess(ClusterMap clusterMap) {
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
      if (clusterMap == null) {
         _log.severe("unlockClusterMap called with null clusterMap arg - ClusterMap lock probably failed");
         return false;
      }
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
