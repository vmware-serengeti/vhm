package com.vmware.vhadoop.vhm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ExecutionStrategy;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;

public class ThreadPoolExecutionStrategy implements ExecutionStrategy, EventProducer {
   private ExecutorService _threadPool;
   private Map<Set<ClusterScaleEvent>, Future<ClusterScaleCompletionEvent>> _runningTasks;
   private Map<String, ScaleStrategyContext> _contextMap;
   private static int _threadCounter = 0;
   private EventConsumer _consumer;
   private Thread _mainThread;
   private boolean _started;

   private static final Logger _log = Logger.getLogger(ThreadPoolExecutionStrategy.class.getName());

   public ThreadPoolExecutionStrategy() {
      _threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "Cluster_Thread_"+(_threadCounter++));
         }
      });
      _runningTasks = new HashMap<Set<ClusterScaleEvent>, Future<ClusterScaleCompletionEvent>>();
   }

   @Override
   public void handleClusterScaleEvents(String clusterId, ScaleStrategy scaleStrategy, Set<ClusterScaleEvent> events) {
      ScaleStrategyContext context = getContextForCluster(clusterId, scaleStrategy.getStrategyContextType());
      Future<ClusterScaleCompletionEvent> task = _threadPool.submit(scaleStrategy.getClusterScaleOperation(clusterId, events, context));
      synchronized(_runningTasks) {
         _runningTasks.put(events, task);
      }
   }

   private ScaleStrategyContext getContextForCluster(String clusterId, Class<? extends ScaleStrategyContext> type) {
      if (type == null) {
         return null;
      }
      if (_contextMap == null) {
         _contextMap = new HashMap<String, ScaleStrategyContext>();
      }
      ScaleStrategyContext context = _contextMap.get(clusterId);
      if (context == null) {
         try {
            context = type.newInstance();
         } catch (Exception e) {
            _log.severe("Cannot instantiate ScaleStrategyContext: "+e.getMessage());
         }
      }
      _contextMap.put(clusterId, context);
      return context;
   }

   @Override
   public void registerEventConsumer(EventConsumer consumer) {
      _consumer = consumer;
   }

   @Override
   public void start() {
      _started = true;
      _mainThread = new Thread(new Runnable() {
         @Override
         public void run() {
            List<Set<ClusterScaleEvent>> toRemove = new ArrayList<Set<ClusterScaleEvent>>();
            List<ClusterScaleCompletionEvent> completedTasks = new ArrayList<ClusterScaleCompletionEvent>();
            synchronized(_runningTasks) {
               while (_started) {
                  for (Set<ClusterScaleEvent> key : _runningTasks.keySet()) {
                     Future<ClusterScaleCompletionEvent> task = _runningTasks.get(key);
                     if (task.isDone()) {
                        try {
                           ClusterScaleCompletionEvent completionEvent = task.get();
                           if (completionEvent != null) {
                              _log.info("Found completed task for cluster <%C"+completionEvent.getClusterId());
                              completedTasks.add(completionEvent);
                           }
                        } catch (InterruptedException e) {
                           _log.warning("Cluster thread interrupted");
                        } catch (ExecutionException e) {
                           _log.warning("ExecutionException in cluster thread: "+e.getMessage());
                           e.printStackTrace();
                        }
                        toRemove.add(key);
                     }
                  }
                  /* Add the completed tasks in one block, ensuring a single ClusterMap update */
                  if (completedTasks.size() > 0) {
                     _consumer.placeEventCollectionOnQueue(completedTasks);
                     completedTasks.clear();
                  }
                  if (toRemove.size() > 0) {
                     for (Set<ClusterScaleEvent> key : toRemove) {
                        _runningTasks.remove(key);
                     }
                     toRemove.clear();
                  }
                  try {
                     _runningTasks.wait(1000);
                  } catch (InterruptedException e) {}
               }
               _log.info("ThreadPoolExecutionStrategy stopping...");
            }
         }
      }, "ScaleStrategyCompletionListener");
      _mainThread.start();
   }

   @Override
   public void stop() {
      _started = false;
      _mainThread.interrupt();
   }

}
