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

import com.vmware.vhadoop.api.vhm.events.NotificationEvent;
import com.vmware.vhadoop.api.vhm.events.PolicyViolationEvent;

/**
 * A PolicyMonitor watches changes in the system state and gets the opportunity to create @PolicyViolationEvent events
 * All @ClusterStateChangeEvent objects are passed to enforcePolicy before they reach the VHM event queue
 */
public interface PolicyMonitor {

   /**
    * When an event arrives on the VHM queue, enforcePolicy is invoked with the event
    * The method should not modify the event passed in
    * @PolicyViolationEvent events returned are placed on the VHM event queue along with the @NotificationEvent passed in
    * 
    * @param csce A @NotificationEvent as it arrives on the VHM queue
    * @return A Set of @PolicyViolationEvents or null
    */
   Set<PolicyViolationEvent> enforcePolicy(final NotificationEvent csce);

}
