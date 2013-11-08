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
package com.vmware.vhadoop.api.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterHealthEvent;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;

/**
 * Component that hooks into the event processing of VHM and looks for issues related to the health of system components
 * HealthManager gets to peek at all events before they are processed by VHM and can return @HealthEvents to be queued
 * HealthManager is a @VMChooser so that it can use the health state it stores to assist in choosing healthy VMs
 * HealthManager has an embedded @HealthMonitor interface which gets access to @HealthEvents as they appear on the VHM queue
 */
public interface HealthManager extends VMChooser {
   /**
    * HealthMonitor is an optional component that can be used to notify external systems of recurring @HealthEvents
    * Every time VHM processes events on its queue, the HealthEvents are passed to the HealthMonitor
    */
   public interface HealthMonitor {

      /* TODO: Should this return anything? */
      void handleHealthEvents(VCActions _vcActions, Set<ClusterHealthEvent> healthEvents);

   }

   HealthMonitor getHealthMonitor();
   
   /**
    * If an event is passed to VHM, checkHealth gets a chance to generate corresponding @ClusterHealthEvent events
    * This happens before the event reaches the VHM queue, to allow @ClusterHealthEvent objects to be injected as early as possible
    * The event passed should not be modified
    * 
    * @param event A @NotificationEvent to review
    * @return @ClusterHealthEvent objects or null
    */
   Set<ClusterHealthEvent> checkHealth(final NotificationEvent event);

}
