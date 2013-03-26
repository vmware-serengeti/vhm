package com.vmware.vhadoop.api.vhm.events;

import java.util.List;

public interface EventConsumer {

   public void placeEventOnQueue(NotificationEvent event);

   public void placeEventCollectionOnQueue(List<? extends NotificationEvent> events);

   public void blockOnEventProcessingCompletion(ClusterScaleEvent event);
}
