package com.vmware.vhadoop.api.vhm.events;

public interface NotificationEvent {
   public boolean getCanClearQueue();
      
   public boolean getCanBeClearedFromQueue();
   
   public boolean isSameEventTypeAs(NotificationEvent next);
}
