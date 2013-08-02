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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.ExecutionStrategy;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;
import com.vmware.vhadoop.vhm.events.AbstractNotificationEvent;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;
import com.vmware.vhadoop.vhm.events.ClusterUpdateEvent;
import com.vmware.vhadoop.vhm.events.NewVmEvent;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.events.VmRemovedFromClusterEvent;
import com.vmware.vhadoop.vhm.events.VmUpdateEvent;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;

public class VHM implements EventConsumer {
   private final EventProducerActions _eventProducers;
   private final Queue<NotificationEvent> _eventQueue;
   private boolean _initialized;
   private final ClusterMapImpl _clusterMap;
   private final ExecutionStrategy _executionStrategy;
   private final VCActions _vcActions;
   private final MultipleReaderSingleWriterClusterMapAccess _clusterMapAccess;
   private final ClusterMapReader _parentClusterMapReader;
   private volatile boolean _running = false;
   private volatile boolean _stopped = true;

   private static final Logger _log = Logger.getLogger(VHM.class.getName());
   private static final long CLUSTER_COMPLETENESS_GRACE_TIME_MILLIS = 10000;

   private static long EVENT_PRODUCER_START_GRACE_TIME_MILLIS = 5000;
   private static long EVENT_PRODUCER_STOP_GRACE_TIME_MILLIS = 5000;

   VHM(VCActions vcActions, ScaleStrategy[] scaleStrategies,
         ExtraInfoToClusterMapper strategyMapper, ThreadLocalCompoundStatus threadLocalStatus) {
      _eventProducers = new EventProducerActions();
      _eventQueue = new LinkedList<NotificationEvent>();
      _initialized = true;
      _clusterMap = new ClusterMapImpl(strategyMapper);
      _vcActions = vcActions;
      _clusterMapAccess = MultipleReaderSingleWriterClusterMapAccess.getClusterMapAccess(_clusterMap);
      _parentClusterMapReader = new AbstractClusterMapReader(_clusterMapAccess, threadLocalStatus) {};
      initScaleStrategies(scaleStrategies);
      _executionStrategy = new ThreadPoolExecutionStrategy();
      if (!registerEventProducer((ThreadPoolExecutionStrategy)_executionStrategy)) {
         throw new RuntimeException("Fatal error registering ThreadPoolExecutionStrategy as an event producer");
      }
   }

   private void initScaleStrategies(ScaleStrategy[] scaleStrategies) {
      for (ScaleStrategy strategy : scaleStrategies) {
         _clusterMap.registerScaleStrategy(strategy);
         strategy.initialize(_parentClusterMapReader);
      }
   }

   private class EventProducerResetEvent extends AbstractNotificationEvent {
      EventProducerResetEvent() {
         super(false, false);
      }
   }

   /* Threading Requirement:
    *  - Protect _eventProducers collection from changing while another thread is iterating over it
    *  - Treat stop() start() and reset() as blocking atomic operations
    *  - Allow threads to figure out the state of the event producers without blocking
    */
   private class EventProducerActions {
      private final Set<EventProducer> _eventProducers = new HashSet<EventProducer>();
      private final Set<EventProducer> _startedProducers = Collections.synchronizedSet(new HashSet<EventProducer>());      /* DO NOT ITERATE */
      private final EventProducer.EventProducerStartStopCallback _startStopHandler = new EventProducerStartStopHandler();

      private class EventProducerStartStopHandler implements EventProducer.EventProducerStartStopCallback {
         @Override
         public void notifyFailed(EventProducer thisProducer) {
            _log.severe("EventProducer "+thisProducer.getClass().getName()+" has stopped unexpectedly, so resetting EventProducers");
            placeEventOnQueue(new EventProducerResetEvent());
         }

         @Override
         public void notifyStarted(EventProducer thisProducer) {
            _startedProducers.add(thisProducer);
            _log.fine("Total started event producers = "+_startedProducers.size());
         }

         @Override
         public void notifyStopped(EventProducer thisProducer) {
            _startedProducers.remove(thisProducer);
            _log.fine("Total started event producers = "+_startedProducers.size());
         }
      }

      /* Block until the event producers to be started have actually stopped */
      synchronized boolean stop() {
         boolean result;
         Set<EventProducer> waitForStop = new HashSet<EventProducer>();
         for (EventProducer eventProducer : _eventProducers) {
            if (_startedProducers.contains(eventProducer)) {
               eventProducer.stop();
               waitForStop.add(eventProducer);
            }
         }
         result = waitForStateChange(waitForStop, false, EVENT_PRODUCER_STOP_GRACE_TIME_MILLIS);
         _log.fine("Event producers stop returning "+result);
         return result;
      }

      /* Block until the event producers to be started have actually started */
      synchronized boolean start() {
         boolean result;
         Set<EventProducer> waitForStart = new HashSet<EventProducer>();
         for (EventProducer eventProducer : _eventProducers) {
            if (!_startedProducers.contains(eventProducer)) {
               eventProducer.start(_startStopHandler);
               waitForStart.add(eventProducer);
            }
         }
         result = waitForStateChange(waitForStart, true, EVENT_PRODUCER_START_GRACE_TIME_MILLIS);
         _log.fine("Event producers start returning "+result);
         return result;
      }

      private boolean waitForStateChange(Set<EventProducer> producers, boolean waitForStart, long timeoutMillis) {
         boolean done;
         int timeoutCountdown = (int)timeoutMillis;
         final int sleepTimeMillis = 100;
         do {
            done = true;
            for (EventProducer producer : producers) {
               if (_startedProducers.contains(producer) != waitForStart) {
                  done = false;
                  break;
               }
            }
            try {
               Thread.sleep(sleepTimeMillis);
            } catch (InterruptedException e) {}
         } while (!done && ((timeoutCountdown -= sleepTimeMillis) > 0));
         return (timeoutCountdown > 0);
      }

      /* Block until the event producers to be started have restarted */
      synchronized boolean reset() {
         if (stop()) {
            if (start()) {
               _log.fine("Event producers successfully restarted");
               return true;
            } else {
               _log.warning("Event producers start failed during reset");
            }
         }
         _log.warning("Event producers stop failed during reset");
         return false;
      }

      synchronized boolean registerNew(EventProducer eventProducer) {
         boolean result = true;
         _eventProducers.add(eventProducer);
         eventProducer.registerEventConsumer(VHM.this);
         if (eventProducer instanceof ClusterMapReader) {
            ((ClusterMapReader)eventProducer).initialize(_parentClusterMapReader);
         }
         eventProducer.start(_startStopHandler);
         Set<EventProducer> singleItemSet = new HashSet<EventProducer>();
         result = waitForStateChange(singleItemSet, true, EVENT_PRODUCER_START_GRACE_TIME_MILLIS);
         _log.fine("Event producer "+eventProducer.getClass().getName()+" registerd. Start result = "+result);
         return result;
      }

      boolean isAllStopped() {
         return _startedProducers.isEmpty();
      }
   }

   private boolean checkForProducerReset(Set<NotificationEvent> events) {
      List<NotificationEvent> toRemove = null;
      for (NotificationEvent notificationEvent : events) {
         if (notificationEvent instanceof EventProducerResetEvent) {
            if (toRemove == null) {
               toRemove = new ArrayList<NotificationEvent>();
            }
            toRemove.add(notificationEvent);
         }
      }
      if (toRemove != null) {
         _log.info("Event Producer reset requested...");
         events.removeAll(toRemove);
         return true;
      }
      return false;
   }

   public boolean registerEventProducer(EventProducer eventProducer) {
      return _eventProducers.registerNew(eventProducer);
   }

   private void addEventToQueue(NotificationEvent event) {
      Queue<NotificationEvent> toKeepQueue = null;
      if (event.getCanClearQueue()) {
         for (NotificationEvent e : _eventQueue) {
            if (!e.getCanBeClearedFromQueue()) {
               if (toKeepQueue == null) {
                  toKeepQueue = new LinkedList<NotificationEvent>();
               }
               toKeepQueue.add(e);
            }
         }
         _eventQueue.clear();
      }
      _eventQueue.add(event);
      if (toKeepQueue != null) {
         _eventQueue.addAll(toKeepQueue);
      }
   }

   /* This can be called by multiple threads */
   @Override
   public void placeEventOnQueue(NotificationEvent event) {
      if (!_initialized) {
         return;
      }
      if (event != null) {
         synchronized(_eventQueue) {
            addEventToQueue(event);
            _eventQueue.notify();
         }
      }
   }

   @Override
   public void placeEventCollectionOnQueue(List<? extends NotificationEvent> events) {
      if (!_initialized) {
         return;
      }
      synchronized(_eventQueue) {
         for (NotificationEvent event : events) {
            addEventToQueue(event);
         }
         _eventQueue.notify();
      }
   }

   public Set<NotificationEvent> pollForEvents() {
      Set<NotificationEvent> results = null;
      synchronized(_eventQueue) {
         while (_eventQueue.peek() == null) {
            try {
               _eventQueue.wait();
            } catch (InterruptedException e) {
               _log.warning("Interrupted unexpectedly while waiting for event");
            }
         }
         results = new LinkedHashSet<NotificationEvent>();
         while (_eventQueue.peek() != null) {
            /* Use of a Set ensured duplicates are eliminated */
            /* TODO: add an event key to do event consolidation. At the moment events use the default equality so this has little effect */
            results.add(_eventQueue.poll());
         }
      }
      return results;
   }

   public NotificationEvent getEventPending() {
      synchronized(_eventQueue) {
         return _eventQueue.peek();
      }
   }

   private String getClusterIdForVCFolder(String folderName) {
      String clusterId = null;
      List<String> vms = _vcActions.listVMsInFolder(folderName);
      /* Returning null may indicate a VC connection failure */
      if (vms != null) {
         clusterId = _clusterMap.getClusterIdFromVMs(vms);
         if (clusterId != null) {
            _clusterMap.associateFolderWithCluster(clusterId, folderName);
         }
      }
      return clusterId;
   }

   /* TODO: Note that currently, this method cannot deal with a clusterScaleEvent with just a hostId
    * We should be able to deal with this at some point - ie: general host contention impacts multiple clusters */
   private String completeClusterScaleEventDetails(AbstractClusterScaleEvent event) {
      String clusterId = event.getClusterId();

      if ((clusterId == null) && (event instanceof SerengetiLimitInstruction)) {
         String clusterFolderName = ((SerengetiLimitInstruction)event).getClusterFolderName();
         if (clusterFolderName != null) {
            clusterId = getClusterIdForVCFolder(clusterFolderName);
         }
      }

      if (clusterId == null) {
         /* Find the clusterId from the VM */
         String hostId = event.getHostId();
         String vmId = event.getVmId();
         /* Find the host if it has not been provided */
         if (hostId == null) {
            if (vmId != null) {
               hostId = _clusterMap.getHostIdForVm(vmId);
               event.setHostId(hostId);
            }
         }
         if (vmId != null) {
            clusterId = _clusterMap.getClusterIdForVm(vmId);
         } else {
            _log.warning("No usable data from ClusterScaleEvent (" +
                  event.getVmId() + "," + event.getHostId() + "," + event.getClusterId() + ")");
            if (event instanceof SerengetiLimitInstruction) {
               SerengetiLimitInstruction sEvent = (SerengetiLimitInstruction)event;
               _log.warning("SerengetiEvent for cluster=" + sEvent.getClusterFolderName());
            }
            _clusterMap.dumpState(Level.WARNING);
         }
      }

      event.setClusterId(clusterId);
      return clusterId;
   }

   private void updateOrCreateClusterScaleEventSet(String clusterId, ClusterScaleEvent newEvent,
         Map<String, Set<ClusterScaleEvent>> clusterScaleEventMap) {
      Set<ClusterScaleEvent> clusterScaleEvents = clusterScaleEventMap.get(clusterId);
      if (clusterScaleEvents == null) {
         clusterScaleEvents = new LinkedHashSet<ClusterScaleEvent>();      /* Preserve order */
         clusterScaleEventMap.put(clusterId, clusterScaleEvents);
      }
      clusterScaleEvents.add(newEvent);
   }

   /* The method takes all new events polled from the event queue, pulls out any ClsuterScaleEvents and organizes them by Cluster */
   private void getQueuedScaleEventsForCluster(Set<NotificationEvent> events, Map<String, Set<ClusterScaleEvent>> clusterScaleEventMap) {
      if (clusterScaleEventMap != null) {
         for (NotificationEvent event : events) {
            if (event instanceof AbstractClusterScaleEvent) {
               /* Derive the cluster ID and other details if the event does not already have it */
               String clusterId = completeClusterScaleEventDetails((AbstractClusterScaleEvent)event);
               updateOrCreateClusterScaleEventSet(clusterId, (ClusterScaleEvent)event, clusterScaleEventMap);
            }
         }
      }
   }

   private Set<ClusterStateChangeEvent> getClusterAddRemoveEvents(Set<NotificationEvent> events) {
      Set<ClusterStateChangeEvent> results = new LinkedHashSet<ClusterStateChangeEvent>();      /* Preserve order */
      for (NotificationEvent event : events) {
         if ((event instanceof NewVmEvent) || (event instanceof VmRemovedFromClusterEvent)) {
            results.add((ClusterStateChangeEvent)event);
         }
      }
      return results;
   }

   private Set<ClusterStateChangeEvent> getClusterUpdateEvents(Set<NotificationEvent> events) {
      Set<ClusterStateChangeEvent> results = new LinkedHashSet<ClusterStateChangeEvent>();      /* Preserve order */
      for (NotificationEvent event : events) {
         if ((event instanceof VmUpdateEvent) || (event instanceof ClusterUpdateEvent)) {
            results.add((ClusterStateChangeEvent)event);
         }
      }
      return results;
   }

   private Set<ClusterScaleCompletionEvent> getClusterScaleCompletionEvents(Set<NotificationEvent> events) {
      Set<ClusterScaleCompletionEvent> results = new LinkedHashSet<ClusterScaleCompletionEvent>();      /* Preserve order */
      for (NotificationEvent event : events) {
         if (event instanceof ClusterScaleCompletionEvent) {
            results.add((ClusterScaleCompletionEvent)event);
         }
      }
      return results;
   }

   private void doRemove(Set<ClusterScaleEvent> scaleEventsForCluster, Set<ClusterScaleEvent> toRemove, String method) {
      if (toRemove != null) {
         int beforeSize = scaleEventsForCluster.size();
         scaleEventsForCluster.removeAll(toRemove);
         int afterSize = scaleEventsForCluster.size();
         _log.info("Consolidating scale events from "+beforeSize+" to "+afterSize+" for method "+method);
      }
   }

   /* Takes a list of types that are allowed for a particular cluster and removes any
    * events that are not of those types, either directly or through inheritance */
   private void removeEventsThisClusterCantHandle(Class<? extends ClusterScaleEvent>[] typesHandled,
                                                  Set<ClusterScaleEvent> scaleEventsForCluster) {
      Set<ClusterScaleEvent> toRemove = null;
      for (ClusterScaleEvent event : scaleEventsForCluster) {
         boolean isAssignableFromAtLeastOne = false;
         for (Class<? extends ClusterScaleEvent> typeHandled : typesHandled) {
            if (typeHandled.isAssignableFrom(event.getClass())) {
               isAssignableFromAtLeastOne = true;
               break;
            }
         }
         if (!isAssignableFromAtLeastOne) {
            _log.finer("Scale strategy cannot handle event "+event);
            if (toRemove == null) {
               toRemove = new HashSet<ClusterScaleEvent>();
            }
            toRemove.add(event);
         }
      }
      doRemove(scaleEventsForCluster, toRemove, "removeEventsThisClusterCantHandle");
   }

   /* If events are marked isExclusive() == true and if there are duplicate
    * events of that type, only the most recent should be returned */
   private void consolidateExclusiveEvents(Set<ClusterScaleEvent> scaleEventsForCluster) {
      Set<ClusterScaleEvent> toRemove = new HashSet<ClusterScaleEvent>();;

      Map<Class<? extends ClusterScaleEvent>, ClusterScaleEvent> newestEventMap =
            new HashMap<Class<? extends ClusterScaleEvent>, ClusterScaleEvent>();
      for (ClusterScaleEvent event : scaleEventsForCluster) {
         if (event.isExclusive()) {
            ClusterScaleEvent toCompare = newestEventMap.get(event.getClass());
            if (toCompare == null) {
               newestEventMap.put(event.getClass(), event);
            } else {
               if (toCompare.getTimestamp() >= event.getTimestamp()) {
                  toRemove.add(event);
               } else {
                  newestEventMap.put(event.getClass(), event);
                  toRemove.add(toCompare);
               }
            }
         }
      }
      if (toRemove.size() > 0) {
         doRemove(scaleEventsForCluster, toRemove, "consolidateExclusiveEvents");
      }
   }

   /* For now, remove any events that the scale strategy is not designed to be able to handle */
   private Set<ClusterScaleEvent> consolidateClusterEvents(ScaleStrategy scaleStrategy, Set<ClusterScaleEvent> scaleEventsForCluster) {
      removeEventsThisClusterCantHandle(scaleStrategy.getScaleEventTypesHandled(), scaleEventsForCluster);
      consolidateExclusiveEvents(scaleEventsForCluster);
      return scaleEventsForCluster;
   }

   private SerengetiLimitInstruction pendingBlockingSwitchToManual(Set<ClusterScaleEvent> consolidatedEvents) {
      for (ClusterScaleEvent clusterScaleEvent : consolidatedEvents) {
         if (clusterScaleEvent instanceof SerengetiLimitInstruction) {
            SerengetiLimitInstruction returnVal = (SerengetiLimitInstruction)clusterScaleEvent;
            if (returnVal.getAction().equals(SerengetiLimitInstruction.actionWaitForManual)) {
               return returnVal;
            }
         }
      }
      return null;
   }

   /* Process new cluster state change events received from the ClusterStateChangeListener
    * The impliedScaleEventsMap allows any clusterScaleEvents implied by cluster state changes to be added */
   private void handleClusterStateChangeEvents(Set<ClusterStateChangeEvent> eventsToProcess,
         Map<String, Set<ClusterScaleEvent>> impliedScaleEventsMap) {
      Set<ClusterScaleEvent> impliedScaleEventsForCluster = new LinkedHashSet<ClusterScaleEvent>();    /* Preserve order */

      for (ClusterStateChangeEvent event : eventsToProcess) {
         _log.info("ClusterStateChangeEvent received: "+event.getClass().getName());

         /* ClusterMap will process the event and may add an implied scale event (see ExtraInfoToClusterMapper) */
         String clusterId = _clusterMap.handleClusterEvent(event, impliedScaleEventsForCluster);
         if (clusterId != null) {

            /* If there are new scale events, create or update the Set in the impliedScaleEventsMap */
            if (impliedScaleEventsForCluster.size() > 0) {
               if (impliedScaleEventsMap.get(clusterId) == null) {
                  impliedScaleEventsMap.put(clusterId, impliedScaleEventsForCluster);
                  impliedScaleEventsForCluster = new LinkedHashSet<ClusterScaleEvent>();
               } else {
                  impliedScaleEventsMap.get(clusterId).addAll(impliedScaleEventsForCluster);
                  impliedScaleEventsForCluster.clear();
               }
            }
         }
      }
   }

   /* When events are polled, this is the first method that gets the opportunity to triage them */
   private void handleEvents(Set<NotificationEvent> events) {
      /* addRemoveEvents are events that affect the shape of a cluster */
      final Set<ClusterStateChangeEvent> addRemoveEvents = getClusterAddRemoveEvents(events);

      /* updateEvents are events that change the state of a cluster */
      final Set<ClusterStateChangeEvent> updateEvents = getClusterUpdateEvents(events);

      /* completionEvents are received when a cluster scale thread has finished executing */
      final Set<ClusterScaleCompletionEvent> completionEvents = getClusterScaleCompletionEvents(events);

      /* clusterScaleEvents are events suggesting the scaling up or down of a cluster */
      final Map<String, Set<ClusterScaleEvent>> clusterScaleEvents = new HashMap<String, Set<ClusterScaleEvent>>();

      /* ClusterMap is updated by VHM based on events that come in from the ClusterStateChangeListener
       * The first thing we do here is update ClusterMap to ensure that the latest state is reflected ASAP
       * Note that clusterScaleEvents can be implied by cluster state changes, so new clusterScaleEvents can be added here */
      if ((addRemoveEvents.size() + updateEvents.size() + completionEvents.size()) > 0) {
         _clusterMapAccess.runCodeInWriteLock(new Callable<Object>() {
            @Override
            public Object call() throws Exception {

               /* Add/remove events are handled first as these will have the most significant impact */
               handleClusterStateChangeEvents(addRemoveEvents, clusterScaleEvents);

               /* Note that the scale strategy key may be updated here so any subsequent call to getScaleStrategyForCluster will reflect the change */
               handleClusterStateChangeEvents(updateEvents, clusterScaleEvents);
               for (ClusterScaleCompletionEvent event : completionEvents) {
                  _log.info("ClusterScaleCompletionEvent received: "+event.getClass().getName());
                  if (event instanceof ClusterScaleDecision) {
                     /* The option exists for a scaleStrategy to ask for an event to be re-queued, causing it to be re-invoked */
                     List<NotificationEvent> eventsToRequeue = ((ClusterScaleDecision)event).getEventsToRequeue();
                     if (eventsToRequeue != null) {
                        _log.info("Requeuing event(s) from ClusterScaleCompletionEvent: "+eventsToRequeue);
                        placeEventCollectionOnQueue(eventsToRequeue);
                     }
                  }
                  _clusterMap.handleCompletionEvent(event);
               }
               return null;
            }
         });
      }

      /* Now that we may have some implied scale events from above, add any additional scale events from the event queue */
      getQueuedScaleEventsForCluster(events, clusterScaleEvents);

      /* If there are scale events to handle, we need to invoke the scale strategies for each cluster
       * The ordering in which we process the clusters doesn't matter as they will be done concurrently */
      if (clusterScaleEvents.size() > 0) {
         for (String clusterId : clusterScaleEvents.keySet()) {
            Set<ClusterScaleEvent> unconsolidatedEvents = clusterScaleEvents.get(clusterId);
            if (unconsolidatedEvents == null) {
               continue;
            }
            /* If ClusterMap has not yet been fully updated with information about a cluster, defer this operation */
            Boolean clusterCompleteness = _clusterMap.validateClusterCompleteness(clusterId, CLUSTER_COMPLETENESS_GRACE_TIME_MILLIS);
            if (clusterCompleteness != null) {
               if (!clusterCompleteness) {
                  if (unconsolidatedEvents.size() > 0) {
                     _log.info("ClusterInfo not yet complete. Putting event collection back on queue for cluster <%C"+clusterId);
                     placeEventCollectionOnQueue(new ArrayList<ClusterScaleEvent>(unconsolidatedEvents));
                  }
                  continue;
               }
            } else {
               _log.warning("Cluster <%C"+clusterId+"%C> has been incomplete for longer than the grace period of "
                                                      +CLUSTER_COMPLETENESS_GRACE_TIME_MILLIS+"ms. Dumping queued events for it");
               continue;
            }

            /* Note that any update to the scale strategy will already have been processed above in handleClusterStateChangeEvents */
            ScaleStrategy scaleStrategy = _clusterMap.getScaleStrategyForCluster(clusterId);
            if (scaleStrategy == null) {
               _log.severe("There is no scaleStrategy set for cluster <%C"+clusterId);
               continue;
            }

            _log.finer("Using "+scaleStrategy.getKey()+" scale strategy to filter events for cluster "+clusterId);

            /* UnconsolidatedEvents guaranteed to be non-null and consolidatedEvents should be a trimmed down version of the same collection */
            Set<ClusterScaleEvent> consolidatedEvents = consolidateClusterEvents(scaleStrategy, unconsolidatedEvents);
            if (consolidatedEvents.size() > 0) {
               /* If there is an instruction from Serengeti to switch to manual, strip out that one event and dump the others */
               SerengetiLimitInstruction switchToManualEvent = pendingBlockingSwitchToManual(consolidatedEvents);
               if (switchToManualEvent != null) {
                  /* If Serengeti has made the necessary change to extraInfo AND any other scaling has completed, inform completion */
                  boolean extraInfoChanged = _clusterMap.getScaleStrategyKey(clusterId).equals(ManualScaleStrategy.MANUAL_SCALE_STRATEGY_KEY);
                  boolean scalingCompleted = !_executionStrategy.isClusterScaleInProgress(clusterId);
                  if (extraInfoChanged && scalingCompleted) {
                     _log.info("Switch to manual scale strategy for cluster <%C"+clusterId+"%C> is now complete. Reporting back to Serengeti");
                     switchToManualEvent.reportCompletion();
                  } else {
                     /* Continue to block Serengeti CLI by putting the event back on the queue */
                     placeEventCollectionOnQueue(Arrays.asList(new ClusterScaleEvent[]{switchToManualEvent}));
                  }
               /* Call out to the execution strategy to handle the scale events for the cluster - non blocking */
               } else if (!_executionStrategy.handleClusterScaleEvents(clusterId, scaleStrategy, consolidatedEvents)) {
                  /* If we couldn't schedule handling of the events, put them back on the queue in their un-consolidated form */
                  _log.finest("Putting event collection back onto VHM queue - size="+unconsolidatedEvents.size());
                  placeEventCollectionOnQueue(new ArrayList<ClusterScaleEvent>(unconsolidatedEvents));
               }
            }
         }
      }
   }

   public Thread start() {
      _stopped = false;
      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            try {
               _running = true;
               while (_running) {
                  Set<NotificationEvent> events = pollForEvents();
                  if (checkForProducerReset(events)) {
                     if (!_eventProducers.reset()) {
                        _log.severe("Unable to reset Event Producers. Terminating VHM");
                        break;
                     }
                  }
                  handleEvents(events);
                  Thread.sleep(500);
               }
            } catch (Throwable e) {
               _log.log(Level.WARNING, "VHM stopping due to exception ", e);
            }
            _log.info("VHM stopping...");
            _stopped = true;
         }}, "VHM_Main_Thread");
      t.start();
      return t;
   }

   public void stop(boolean hardStop) {
      _log.log(VhmLevel.USER, "VHM: stopping");
      _running = false;
      _eventProducers.stop();
      placeEventOnQueue(new AbstractNotificationEvent(hardStop, false) {});
   }


   public boolean isStopped() {
      return (_stopped && _eventProducers.isAllStopped());
   }

   VCActions getVCActions() {
      return _vcActions;
   }

   /**
    * Hack to specifically dump the cluster map instead of a more generic state of the world
    *
    */
   public void dumpClusterMap(Level level) {
      _clusterMap.dumpState(level);
   }
}
