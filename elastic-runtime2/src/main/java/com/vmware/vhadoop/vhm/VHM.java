package com.vmware.vhadoop.vhm;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.*;
import com.vmware.vhadoop.api.vhm.ClusterMapReader.ClusterMapAccess;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.events.SerengetiLimitEvent;
import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;
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
      _executionStrategy = new ThreadPoolExecutionStrategy();
      _clusterMapReaderCntr = new AtomicInteger();
      _clusterMapWriteLock = new Object();
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
      eventProducer.registerClusterMapAccess(new MultipleReaderSingleWriterClusterMapAccess());
      eventProducer.start();
   }

   /* This can be called by multiple threads */
   @Override
   public void placeEventOnQueue(NotificationEvent event) {
      if (!_initialized) {
         return;
      }
      if (event != null) {
         Queue<NotificationEvent> toKeepQueue = null;
         synchronized(_eventQueue) {
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
            _eventQueue.notify();
         }
      }
   }
   
   @Override
   public void placeEventCollectionOnQueue(List<? extends NotificationEvent> events) {
      // TODO Auto-generated method stub
      
   }

   public NotificationEvent pollForEvent() {
      NotificationEvent result, next;
      synchronized(_eventQueue) {
         if (_eventQueue.peek() == null) {
            try {
               _eventQueue.wait();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
         result = _eventQueue.poll();
         next = _eventQueue.peek();
         /* Cycle through duplicate events to the most recent */
         while ((next != null) && result.getCanBeClearedFromQueue() && 
               result.isSameEventTypeAs(next)) {
            result = _eventQueue.poll();
            next = _eventQueue.peek();
         }
         return result;
      }
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
   
   private void handleEvent(NotificationEvent event) {
      if (event instanceof ClusterScaleEvent) {
         _log.info("ClusterScaleEvent received: "+event.getClass().getName());
         String clusterFolderName = ((ClusterScaleEvent)event).getClusterFolderName();
         String clusterId = getClusterIdForVCFolder(clusterFolderName);
         if (clusterId != null) {
            ScaleStrategy scaleStrategy = _clusterMap.getScaleStrategyForCluster(clusterId);
            _executionStrategy.handleClusterScaleEvent(scaleStrategy, (ClusterScaleEvent)event);
         }
      } else 
      if (event instanceof ClusterStateChangeEvent) {
         _log.info("ClusterStateChangeEvent received: "+event.getClass().getName());
         synchronized (_clusterMapWriteLock) {
            /* Wait for the readers to stop reading. New readers will block on the write lock */
            while (_clusterMapReaderCntr.get() > 0) {
               try {
                  Thread.sleep(10);
               } catch (InterruptedException e) {}
            }
            _clusterMap.handleClusterEvent((ClusterStateChangeEvent)event);
         }
      } else {
         System.out.println("No events polled");
      }
   }
   
   public void start() {
      new Thread(new Runnable() {
         @Override
         public void run() {
            while (true) {
               NotificationEvent event = pollForEvent();
               handleEvent(event);
            }
         }}, "VHM_Main_Thread").start();
   }
   
   VCActions getVCActions() {
      return _vcActions;
   }

   @Override
   public void blockOnEventProcessingCompletion(ClusterScaleEvent event) {
      _executionStrategy.waitForClusterScaleCompletion(event);
   }
}
