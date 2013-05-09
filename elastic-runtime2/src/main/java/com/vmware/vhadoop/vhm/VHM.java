package com.vmware.vhadoop.vhm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToScaleStrategyMapper;
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
import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;

public class VHM implements EventConsumer {
   private Set<EventProducer> _eventProducers;
   private Queue<NotificationEvent> _eventQueue;
   private boolean _initialized;
   private ClusterMapImpl _clusterMap;
   private ExecutionStrategy _executionStrategy;
   private VCActions _vcActions;
   private MultipleReaderSingleWriterClusterMapAccess _clusterMapAccess;
   private ClusterMapReader _parentClusterMapReader;

   private static final Logger _log = Logger.getLogger(VHM.class.getName());

   public VHM(VCActions vcActions, ScaleStrategy[] scaleStrategies,
         ExtraInfoToScaleStrategyMapper strategyMapper, ThreadLocalCompoundStatus threadLocalStatus) {
      _eventProducers = new HashSet<EventProducer>();
      _eventQueue = new LinkedList<NotificationEvent>();
      _initialized = true;
      _clusterMap = new ClusterMapImpl(strategyMapper);
      _vcActions = vcActions;
      _clusterMapAccess = MultipleReaderSingleWriterClusterMapAccess.getClusterMapAccess(_clusterMap);
      _parentClusterMapReader = new AbstractClusterMapReader(_clusterMapAccess, threadLocalStatus) {};
      initScaleStrategies(scaleStrategies);
      _executionStrategy = new ThreadPoolExecutionStrategy();
      registerEventProducer((ThreadPoolExecutionStrategy)_executionStrategy);
   }

   private void initScaleStrategies(ScaleStrategy[] scaleStrategies) {
      for (ScaleStrategy strategy : scaleStrategies) {
         _clusterMap.registerScaleStrategy(strategy);
         strategy.initialize(_parentClusterMapReader);
      }
   }

   public void registerEventProducer(EventProducer eventProducer) {
      _eventProducers.add(eventProducer);
      eventProducer.registerEventConsumer(this);
      if (eventProducer instanceof ClusterMapReader) {
         ((ClusterMapReader)eventProducer).initialize(_parentClusterMapReader);
      }
      eventProducer.start();
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
      HashSet<NotificationEvent> results = null;
      synchronized(_eventQueue) {
         while (_eventQueue.peek() == null) {
            try {
               _eventQueue.wait();
            } catch (InterruptedException e) {
               _log.warning("Interrupted unexpectedly while waiting for event");
            }
         }
         results = new HashSet<NotificationEvent>();
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
      List<String> vms = _vcActions.listVMsInFolder(folderName);
      return _clusterMap.getClusterIdFromVMsInFolder(folderName, vms);
   }

   /* TODO: Note that currently, this method cannot deal with a clusterScaleEvent with just a hostId
    * We should be able to deal with this at some point - ie: general host contention impacts multiple clusters */
   private String completeClusterScaleEventDetails(AbstractClusterScaleEvent event) {
      String clusterId = event.getClusterId();

      if (event instanceof SerengetiLimitInstruction) {
         clusterId = getClusterIdForVCFolder(((SerengetiLimitInstruction)event).getClusterFolderName());
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

   private Map<String, Set<ClusterScaleEvent>> getScaleEventsForCluster(Set<NotificationEvent> events) {
      Map<String, Set<ClusterScaleEvent>> results = new HashMap<String, Set<ClusterScaleEvent>>();
      for (NotificationEvent event : events) {
         if (event instanceof AbstractClusterScaleEvent) {
            String clusterId = completeClusterScaleEventDetails((AbstractClusterScaleEvent)event);
            if (clusterId != null) {
               Set<ClusterScaleEvent> clusterScaleEvents = results.get(clusterId);
               if (clusterScaleEvents == null) {
                  clusterScaleEvents = new HashSet<ClusterScaleEvent>();
                  results.put(clusterId, clusterScaleEvents);
               }
               clusterScaleEvents.add((ClusterScaleEvent)event);
            }
         }
      }
      return results;
   }

   private Set<ClusterStateChangeEvent> getClusterStateChangeEvents(Set<NotificationEvent> events) {
      Set<ClusterStateChangeEvent> results = new HashSet<ClusterStateChangeEvent>();
      for (NotificationEvent event : events) {
         if (event instanceof ClusterStateChangeEvent) {
            results.add((ClusterStateChangeEvent)event);
         }
      }
      return results;
   }

   private Set<ClusterScaleCompletionEvent> getClusterScaleCompletionEvents(Set<NotificationEvent> events) {
      Set<ClusterScaleCompletionEvent> results = new HashSet<ClusterScaleCompletionEvent>();
      for (NotificationEvent event : events) {
         if (event instanceof ClusterScaleCompletionEvent) {
            results.add((ClusterScaleCompletionEvent)event);
         }
      }
      return results;
   }

   private Set<ClusterScaleEvent> consolidateClusterEvents(Set<ClusterScaleEvent> scaleEventsForCluster) {
      /* TODO: Some consolidation logic */
      return scaleEventsForCluster;
   }

   private void handleEvents(Set<NotificationEvent> events) {
      final Set<ClusterStateChangeEvent> clusterStateChangeEvents = getClusterStateChangeEvents(events);
      final Set<ClusterScaleCompletionEvent> completionEvents = getClusterScaleCompletionEvents(events);

      /* Update ClusterMap first */
      if ((clusterStateChangeEvents.size() + completionEvents.size()) > 0) {
         _clusterMapAccess.runCodeInWriteLock(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
               for (ClusterStateChangeEvent event : clusterStateChangeEvents) {
                  _log.info("ClusterStateChangeEvent received: "+event.getClass().getName());
                  _clusterMap.handleClusterEvent(event);
               }
               for (ClusterScaleCompletionEvent event : completionEvents) {
                  _log.info("ClusterScaleCompletionEvent received: "+event.getClass().getName());
                  _clusterMap.handleCompletionEvent(event);
               }
               return null;
            }
         });
      }

      Map<String, Set<ClusterScaleEvent>> clusterScaleEvents = getScaleEventsForCluster(events);

      if (clusterScaleEvents.size() > 0) {
         for (String clusterId : clusterScaleEvents.keySet()) {
            Set<ClusterScaleEvent> consolidatedEvents = consolidateClusterEvents(clusterScaleEvents.get(clusterId));
            if (consolidatedEvents.size() > 0) {
               ScaleStrategy scaleStrategy = _clusterMap.getScaleStrategyForCluster(clusterId);
               _executionStrategy.handleClusterScaleEvents(clusterId, scaleStrategy, consolidatedEvents);
            }
         }
      }
   }

   public Thread start() {
      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            while (true) {
               Set<NotificationEvent> events = pollForEvents();
               handleEvents(events);
            }
         }}, "VHM_Main_Thread");
      t.start();
      return t;
   }

   VCActions getVCActions() {
      return _vcActions;
   }

}
