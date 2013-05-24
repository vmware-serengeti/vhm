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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ExecutionStrategy;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;

public class ThreadPoolExecutionStrategy implements ExecutionStrategy, EventProducer {
   
   private class ClusterTaskContext {
      ScaleStrategy _scaleStrategy;
      Future<ClusterScaleCompletionEvent> _completionEventPending;
      ScaleStrategyContext _scaleStrategyContext;
   }
   
   private ExecutorService _threadPool;
   private Map<String, ClusterTaskContext> _clusterTaskContexts;
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
      _clusterTaskContexts = new HashMap<String, ClusterTaskContext>();
   }
   
   private void setScaleStrategyAndContext(ScaleStrategy scaleStrategy, ClusterTaskContext toSet) throws Exception {
      Class<? extends ScaleStrategyContext> type = scaleStrategy.getStrategyContextType();
      if (type != null) {
         toSet._scaleStrategyContext = type.newInstance();
      }
      toSet._scaleStrategy = scaleStrategy;
   }

   /* ClusterTaskContext represents the state of a running task on a cluster */
   private ClusterTaskContext getClusterTaskContext(String clusterId, ScaleStrategy scaleStrategy) throws Exception {
      synchronized(_clusterTaskContexts) {
         ClusterTaskContext result = _clusterTaskContexts.get(clusterId);
         if (result == null) {
            result = new ClusterTaskContext();
            setScaleStrategyAndContext(scaleStrategy, result);
            _clusterTaskContexts.put(clusterId, result);
            /* If we're switching strategy, we need to reset the context */
         } else if (scaleStrategy != result._scaleStrategy) {
            setScaleStrategyAndContext(scaleStrategy, result);
         }
         return result;
      }
   }

   @Override
   /* This is only ever invoked by the VHM main thread */
   /* Returns true if the events are being handled, false if this is not possible */
   public boolean handleClusterScaleEvents(String clusterId, ScaleStrategy scaleStrategy, Set<ClusterScaleEvent> events) {
      synchronized(_clusterTaskContexts) {
         ClusterTaskContext ctc = null;
         boolean result = false;
         try {
            ctc = getClusterTaskContext(clusterId, scaleStrategy);
         } catch (Exception e) {
            _log.log(Level.SEVERE, "Unexpected exception initializing ClusterTaskContext", e);
         }
         if (ctc._completionEventPending != null) {
            _log.info("Cluster scale event already being handled for cluster <%C"+clusterId);
         } else {
            ctc._completionEventPending = 
                  _threadPool.submit(scaleStrategy.getClusterScaleOperation(clusterId, events, ctc._scaleStrategyContext));
            result = true;
         }
         return result;
      }
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
            List<ClusterScaleCompletionEvent> completedTasks = new ArrayList<ClusterScaleCompletionEvent>();
            synchronized(_clusterTaskContexts) {
               while (_started) {
                  for (String clusterId : _clusterTaskContexts.keySet()) {
                     ClusterTaskContext ctc = _clusterTaskContexts.get(clusterId);
                     if (ctc._completionEventPending != null) {
                        Future<ClusterScaleCompletionEvent> task = ctc._completionEventPending;
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
                           ctc._completionEventPending = null;
                        }
                     }
                  }
                  /* Add the completed tasks in one block, ensuring a single ClusterMap update */
                  if (completedTasks.size() > 0) {
                     _consumer.placeEventCollectionOnQueue(completedTasks);
                     completedTasks.clear();
                  }
                  try {
                     _clusterTaskContexts.wait(500);
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
