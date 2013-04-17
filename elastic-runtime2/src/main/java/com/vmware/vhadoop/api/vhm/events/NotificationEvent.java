package com.vmware.vhadoop.api.vhm.events;

/* Superclass of all events produced by EventProducers and consumed by EventConsumers */
public interface NotificationEvent {
   public boolean getCanClearQueue();
      
   public boolean getCanBeClearedFromQueue();
   
   public boolean isSameEventTypeAs(NotificationEvent next);
   
   public long getTimestamp();
}
