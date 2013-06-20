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
