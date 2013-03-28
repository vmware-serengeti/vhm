package com.vmware.vhadoop.vhm;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.*;
import com.vmware.vhadoop.api.vhm.ClusterMapReader.ClusterMapAccess;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.strategy.DumbEDPolicy;
import com.vmware.vhadoop.vhm.strategy.DumbVMChooser;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;

public class VHM implements EventConsumer {
   private Set<EventProducer> _eventProducers;
   private Queue<NotificationEvent> _eventQueue;
   private boolean _initialized;
   private ClusterMapImpl _clusterMap;
   private ExecutionStrategy _executionStrategy;
   private VCActions _vcActions;
   private static AtomicInteger _clusterMapReaderCntr;
   private Object _clusterMapWriteLock;

   private static final Logger _log = Logger.getLogger(VHM.class.getName());

   public VHM(VCActions vcActions) {
      _eventProducers = new HashSet<EventProducer>();
      _eventQueue = new LinkedList<NotificationEvent>();
      _initialized = true;
      _clusterMap = new ClusterMapImpl();
      _vcActions = vcActions;
      initScaleStrategies();
      _clusterMapReaderCntr = new AtomicInteger();
      _clusterMapWriteLock = new Object();
      _executionStrategy = new ThreadPoolExecutionStrategy();
      registerEventProducer((ThreadPoolExecutionStrategy)_executionStrategy);
   }
   
   private void initScaleStrategies() {
      ScaleStrategy manual = new ManualScaleStrategy(new DumbVMChooser(), new DumbEDPolicy(_vcActions));
      _clusterMap.registerScaleStrategy("manual", manual);      /* TODO: Key should match key in VC cluster info */
      manual.registerClusterMapAccess(new MultipleReaderSingleWriterClusterMapAccess());
   }

   /* Represents multi-threaded read-only access to the ClusterMap. There should be one of these objects per thread */
   public class MultipleReaderSingleWriterClusterMapAccess implements ClusterMapAccess {
      private ClusterMap _locked;
      private Thread _lockedBy;
      
      @Override
      /* If a ClusterMapReader needs to have multiple threads accessing ClusterMap, it should hand out clones */
      public ClusterMapAccess clone() {
         return new MultipleReaderSingleWriterClusterMapAccess();
      }
      
      @Override
      public ClusterMap lockClusterMap() {
         if (_locked == null) {
            /* All readers will be blocked during a ClusterMap write and while ClusterMap waits for the reader count to go to zero */
            synchronized(_clusterMapWriteLock) {
               _locked = _clusterMap;
               _lockedBy = Thread.currentThread();
               _clusterMapReaderCntr.incrementAndGet();
            }
            return _locked;
         } else {
            throw new RuntimeException("Attempt to double-lock ClusterMap!");
         }
      }

      @Override
      public void unlockClusterMap(ClusterMap clusterMap) {
         if (_locked != null) {
            if (_lockedBy == Thread.currentThread()) {
               _locked = null;
               _clusterMapReaderCntr.decrementAndGet();
            } else {
               throw new RuntimeException("Wrong thread trying to unlock ClusterMap!");
            }
         } else {
            throw new RuntimeException("Attempt to double-unlock ClusterMap!");
         }
      }
   }
   
   public void registerEventProducer(EventProducer eventProducer) {
      _eventProducers.add(eventProducer);
      eventProducer.registerEventConsumer(this);
      if (eventProducer instanceof ClusterMapReader) {
         ((ClusterMapReader)eventProducer).registerClusterMapAccess(new MultipleReaderSingleWriterClusterMapAccess());
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
         if (_eventQueue.peek() == null) {
            try {
               _eventQueue.wait();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
         results = new HashSet<NotificationEvent>();
         while (_eventQueue.peek() != null) {
            /* Use of a Set ensured duplicates are eliminated */
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
            /* TODO: Make this more friendly - log and handle error */
            throw new RuntimeException("No usable data from ClusterScaleEvent");
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
            Set<ClusterScaleEvent> clusterScaleEvents = results.get(clusterId);
            if (clusterScaleEvents == null) {
               clusterScaleEvents = new HashSet<ClusterScaleEvent>();
               results.put(clusterId, clusterScaleEvents);
            }
            clusterScaleEvents.add((ClusterScaleEvent)event);
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
      Set<ClusterStateChangeEvent> clusterStateChangeEvents = getClusterStateChangeEvents(events);
      Set<ClusterScaleCompletionEvent> completionEvents = getClusterScaleCompletionEvents(events);

      /* Update ClusterMap first */
      if ((clusterStateChangeEvents.size() + completionEvents.size()) > 0) {
         synchronized (_clusterMapWriteLock) {
            /* Wait for the readers to stop reading. New readers will block on the write lock */
            while (_clusterMapReaderCntr.get() > 0) {
               try {
                  Thread.sleep(10);
               } catch (InterruptedException e) {}
            }
            for (ClusterStateChangeEvent event : clusterStateChangeEvents) {
               _log.info("ClusterStateChangeEvent received: "+event.getClass().getName());
               _clusterMap.handleClusterEvent(event);
            }
            for (ClusterScaleCompletionEvent event : completionEvents) {
               _log.info("ClusterScaleCompletionEvent received: "+event.getClass().getName());
               _clusterMap.handleCompletionEvent(event);
            }
         }
      }
      
      Map<String, Set<ClusterScaleEvent>> clusterScaleEvents = getScaleEventsForCluster(events);

      if (clusterScaleEvents.size() > 0) {
         for (String clusterId : clusterScaleEvents.keySet()) {
            Set<ClusterScaleEvent> consolidatedEvents = consolidateClusterEvents(clusterScaleEvents.get(clusterId));
            if (consolidatedEvents.size() > 0) {
               ScaleStrategy scaleStrategy = _clusterMap.getScaleStrategyForCluster(clusterId);
               _executionStrategy.handleClusterScaleEvents(scaleStrategy, consolidatedEvents);
            }
         }
      }
   }

   public void start() {
      new Thread(new Runnable() {
         @Override
         public void run() {
            while (true) {
               Set<NotificationEvent> events = pollForEvents();
               handleEvents(events);
            }
         }}, "VHM_Main_Thread").start();
   }
   
   VCActions getVCActions() {
      return _vcActions;
   }

}
