package com.vmware.vhadoop.api.vhm.events;

import java.util.List;

/* High level interface, representing the relationship between an EventConsumer and EventProducer
 * The VHM is an EventConsumer and has a number of EventProducers which are wired into it */
public interface EventConsumer {

   public void placeEventOnQueue(NotificationEvent event);

   /* Adding a block of events guarantees that any event consolidation will occur before they can be processed */
   public void placeEventCollectionOnQueue(List<? extends NotificationEvent> events);
}
