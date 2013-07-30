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

   private final ExecutorService _threadPool;
   private final Map<String, ClusterTaskContext> _clusterTaskContexts;
   private static int _threadCounter = 0;
   private EventConsumer _consumer;
   private Thread _mainThread;
   private volatile boolean _started;

   long _startTime = System.currentTimeMillis();
   boolean _deliberateFailureTriggered = false;

   @SuppressWarnings("unused")
   private void deliberatelyFail(long afterTimeMillis) {
      if (!_deliberateFailureTriggered && (System.currentTimeMillis() > (_startTime + afterTimeMillis))) {
         _deliberateFailureTriggered = true;
         throw new RuntimeException("Deliberate failure!!");
      }
   }

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
            if (ctc._completionEventPending != null) {
               _log.finest("Cluster scale events already being handled for cluster <%C"+clusterId);
            } else {
               ctc._completionEventPending =
                     _threadPool.submit(scaleStrategy.getClusterScaleOperation(clusterId, events, ctc._scaleStrategyContext));
               result = true;
            }
         } catch (Exception e) {
            _log.log(Level.SEVERE, "Unexpected exception initializing ClusterTaskContext", e);
         }
         return result;
      }
   }

   @Override
   public void registerEventConsumer(EventConsumer consumer) {
      _consumer = consumer;
   }

   @Override
   public void start(final EventProducerStartStopCallback startStopCallback) {
      _started = true;
      _mainThread = new Thread(new Runnable() {
         @Override
         public void run() {
            List<ClusterScaleCompletionEvent> completedTasks = new ArrayList<ClusterScaleCompletionEvent>();
            synchronized(_clusterTaskContexts) {
               try {
                  _log.info("ThreadPoolExecutionStrategy starting...");
                  startStopCallback.notifyStarted(ThreadPoolExecutionStrategy.this);
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
                                 _log.log(Level.WARNING, "ExecutionException in cluster thread ", e);
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
                     } catch (InterruptedException e) {
                        if (_started) {
                           /* if we're not stopping then this is unexpected */
                           _log.warning("Cluster thread wait interrupted");
                        }
                     }
                  }
               } catch (Throwable t) {
                  _log.log(Level.SEVERE, "Unexpected exception in ThreadPoolExecutionStrategy", t);
                  startStopCallback.notifyFailed(ThreadPoolExecutionStrategy.this);
               }
               _log.info("ThreadPoolExecutionStrategy stopping...");
               startStopCallback.notifyStopped(ThreadPoolExecutionStrategy.this);
            }
         }
      }, "ScaleStrategyCompletionListener");
      _mainThread.start();
   }

   @Override
   public void stop() {
      /* TODO: Although this stops the TPES, the scaling threads its managing are possibly still running - should we block? */
      _started = false;
      _mainThread.interrupt();
   }

   @Override
   public boolean isClusterScaleInProgress(String clusterId) {
      synchronized(_clusterTaskContexts) {
         ClusterTaskContext ctc = _clusterTaskContexts.get(clusterId);
         /* It's ok for there to be no ClusterTaskContext yet as they are created lazily */
         if (ctc != null) {
            /* TODO: Add isAlive() check for the thread - if it has crashed, this isn't enough */
            return ctc._completionEventPending != null;
         }
      }
      return false;
   }

   @Override
   public boolean isStopped() {
      if ((_mainThread == null) || (!_mainThread.isAlive())) {
         return true;
      }
      return false;
   }
}
