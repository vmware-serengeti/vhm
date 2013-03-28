package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.NotificationEvent;

public abstract class AbstractNotificationEvent implements NotificationEvent {
   private final boolean _canClearQueue;
   private final boolean _canBeClearedFromQueue;
   private String _shortName;
   private final long _timestampCreated;
   
   public AbstractNotificationEvent(boolean canClearQueue, boolean canBeClearedFromQueue) {
      _canClearQueue = canClearQueue;
      _canBeClearedFromQueue = canBeClearedFromQueue;
      _timestampCreated = System.currentTimeMillis();
   }
   
   @Override
   public boolean getCanClearQueue() {
      return _canClearQueue;
   }
      
   @Override
   public boolean getCanBeClearedFromQueue() {
      return _canBeClearedFromQueue;
   }

   @Override
   public String toString() {
      if (_shortName == null) {
         String className = getClass().getName();
         if (className.indexOf('$') > 0) {
            _shortName = className.substring(className.lastIndexOf('$')+1);   /* Only works as inner class */
         } else {
            _shortName = className;
         }
      }
      return _shortName;
   }

   @Override
   public boolean isSameEventTypeAs(com.vmware.vhadoop.api.vhm.events.NotificationEvent next) {
      return this.getClass().equals(next.getClass());
   }
   
   @Override
   public long getTimestamp() {
      return _timestampCreated;
   }
}
