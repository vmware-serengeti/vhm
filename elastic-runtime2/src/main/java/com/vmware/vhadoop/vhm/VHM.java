package com.vmware.vhadoop.vhm;

import java.util.*;

import com.vmware.vhadoop.api.vhm.*;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;

public class VHM implements EventConsumer {
   private Set<EventProducer> _eventProducers;
   private Queue<NotificationEvent> _eventQueue;
   private boolean _initialized;
   private ClusterMapImpl _clusterMap;
   private ExecutionStrategy _executionStrategy;
   private VCActions _vcActions;
   
   public VHM() {
      _eventProducers = new HashSet<EventProducer>();
      _eventQueue = new LinkedList<NotificationEvent>();
      _initialized = true;
      _clusterMap = new ClusterMapImpl();
      _vcActions = new VCTestModel();
      initScaleStrategies();
      _executionStrategy = new ThreadPoolExecutionStrategy();
   }
   
   private void initScaleStrategies() {
      ScaleStrategy manual = new ManualScaleStrategy(new DumbVMChooser(), new DumbEDPolicy(_vcActions));
      _clusterMap.registerScaleStrategy("manual", manual);      /* TODO: Key should match key in VC cluster info */
      manual.registerClusterMapAccess(new ClusterMapAccess());
   }
   
   /* Represents multi-threaded read-only access to the ClusterMap */
   public class ClusterMapAccess implements com.vmware.vhadoop.api.vhm.ClusterMapReader.ClusterMapAccess {
      @Override
      public com.vmware.vhadoop.api.vhm.ClusterMap accessClusterMap() {
         /* TODO: Not theadsafe! */
         return _clusterMap;
      }
   }
   
   public void registerEventProducer(EventProducer eventProducer) {
      _eventProducers.add(eventProducer);
      eventProducer.registerConsumer(this);
      eventProducer.registerClusterMapAccess(new ClusterMapAccess());
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
   
   private void handleEvent(NotificationEvent event) {
      if (event instanceof ClusterScaleEvent) {
         System.out.println(Thread.currentThread().getName()+": VHM: ClusterScaleEvent received: "+event.getClass().getName());
         String clusterId = ((ClusterScaleEvent)event).getClusterId();
         ScaleStrategy scaleStrategy = _clusterMap.getScaleStrategyForCluster(clusterId);
         _executionStrategy.handleClusterScaleEvent(scaleStrategy, (ClusterScaleEvent)event);
      } else 
      if (event instanceof ClusterStateChangeEvent) {
         System.out.println(Thread.currentThread().getName()+": VHM: ClusterStateChangeEvent received: "+event.getClass().getName());
         _clusterMap.handleClusterEvent((ClusterStateChangeEvent)event);
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

   public void waitForClusterScaleCompletion() {
      while (getEventPending() != null) {
         try {
            Thread.sleep(100);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
      _executionStrategy.waitForClusterScaleCompletion();
   }
   
   VCActions getVCActions() {
      return _vcActions;
   }
}
