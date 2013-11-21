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
package com.vmware.vhadoop.api.vhm.events;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.VHMCollaborator;

/**
 * An EventInjector is a VHM collaborator that gets to peek at new events and inject further new events in response
 * 
 * VHM can have any number of event injectors registered and the ordering in which they are registered is irrelevant
 *   since they do not get to see each others injected events
 * Events are passed to the EventInjectors after ClusterStateChangeEvents have been processed in ClusterMap but before
 *   any other events have been processed. Any EventInjectors that are also ClusterMapReaders therefore get to see the
 *   net effect the event(s) have had on ClusterMap by the time they are being processed.
 * 
 * @author bcorrie
 *
 */
public interface EventInjector extends VHMCollaborator {

   /**
    * Opportunity to peek at a new Event being processed by ClusterMap
    * Method should return a set of new events or null
    * New events returned do not initially go onto the VHM event queue but may end up being re-queued if they cannot be processed
    */
   public Set<? extends NotificationEvent> processEvent(NotificationEvent event);

   public String getName();
}
