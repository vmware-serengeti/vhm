package com.vmware.vhadoop.vhm;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.*;

import com.vmware.vhadoop.api.vhm.ExecutionStrategy;
import com.vmware.vhadoop.api.vhm.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;


public class ThreadPoolExecutionStrategy implements ExecutionStrategy {
   ExecutorService _threadPool;
   Object _runningTaskLock;
   Set<Future<Object>> _runningTasks;
   static int _threadCounter = 0;

   public ThreadPoolExecutionStrategy() {
      _threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "Cluster_Thread_"+(_threadCounter++));
         }
      });
      _runningTaskLock = new Object();
      _runningTasks = new HashSet<Future<Object>>();
   }
   
   @Override
   public void handleClusterScaleEvent(ScaleStrategy scaleStrategy, ClusterScaleEvent event) {
      Future<Object> task = _threadPool.submit(scaleStrategy.getCallable(event));
      synchronized(_runningTaskLock) {
         _runningTasks.add(task);
      }
   }

   private Object blockOnRunningTask(Future<Object> task) {
      try {
         Object result = task.get();
         synchronized(_runningTaskLock) {
            _runningTasks.remove(task);
         }
      } catch (InterruptedException e) {
         e.printStackTrace();
      } catch (ExecutionException e) {
         e.printStackTrace();
      }
      return null;
   }
   
   @Override
   public void waitForClusterScaleCompletion() {
      Set<Future<Object>> snapshot;
      synchronized(_runningTaskLock) {
         snapshot = new HashSet(_runningTasks);
      }
      for (Future<Object> task : snapshot) {
         blockOnRunningTask(task);
      }
   }

}
