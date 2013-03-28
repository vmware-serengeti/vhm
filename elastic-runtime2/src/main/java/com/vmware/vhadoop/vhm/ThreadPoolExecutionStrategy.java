package com.vmware.vhadoop.vhm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;
import java.util.*;

import com.vmware.vhadoop.api.vhm.ExecutionStrategy;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;

public class ThreadPoolExecutionStrategy implements ExecutionStrategy {
   ExecutorService _threadPool;
   Object _runningTaskLock;
   Map<Set<ClusterScaleEvent>, Future<Object>> _runningTasks;
   static int _threadCounter = 0;

   private static final Logger _log = Logger.getLogger(ThreadPoolExecutionStrategy.class.getName());

   public ThreadPoolExecutionStrategy() {
      _threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "Cluster_Thread_"+(_threadCounter++));
         }
      });
      _runningTaskLock = new Object();
      _runningTasks = new HashMap<Set<ClusterScaleEvent>, Future<Object>>();
   }
   
   @Override
   public void handleClusterScaleEvents(ScaleStrategy scaleStrategy, Set<ClusterScaleEvent> events) {
      Future<Object> task = _threadPool.submit(scaleStrategy.getCallable(events));
      synchronized(_runningTaskLock) {
         _runningTasks.put(events, task);
      }
   }

}
