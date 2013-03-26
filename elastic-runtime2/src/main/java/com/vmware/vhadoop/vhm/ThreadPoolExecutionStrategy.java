package com.vmware.vhadoop.vhm;

import java.util.concurrent.ExecutionException;
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
   Map<ClusterScaleEvent, Future<Object>> _runningTasks;
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
      _runningTasks = new HashMap<ClusterScaleEvent, Future<Object>>();
   }
   
   @Override
   public void handleClusterScaleEvent(ScaleStrategy scaleStrategy, ClusterScaleEvent event) {
      _log.info("Handle cluster scale event for "+event.hashCode());
      Future<Object> task = _threadPool.submit(scaleStrategy.getCallable(event));
      synchronized(_runningTaskLock) {
         _log.info("Associating task "+task.hashCode()+" with event "+event.hashCode());
         _runningTasks.put(event, task);
      }
   }

   @Override
   public void waitForClusterScaleCompletion(ClusterScaleEvent event) {
      Future<Object> task;
      try {
         synchronized(_runningTaskLock) {
            _log.info("wait for completion of "+event.hashCode());
            do {
               task = _runningTasks.get(event);
               _runningTaskLock.wait(100);
               /* TODO: Potenital infinite loop */
            } while (task == null);
            _log.info("done waiting for completion of "+event.hashCode());
         }
         Object result = task.get();
         synchronized(_runningTaskLock) {
            _runningTasks.remove(event);
         }
      } catch (InterruptedException e) {
         e.printStackTrace();
      } catch (ExecutionException e) {
         e.printStackTrace();
      }
   }

}
