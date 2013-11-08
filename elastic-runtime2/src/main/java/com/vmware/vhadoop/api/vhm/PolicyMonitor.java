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

import java.util.List;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.PolicyViolationEvent;

/**
 * A PolicyMonitor watches changes in the system state and gets the opportunity to create @PolicyViolationEvent events
 * All @ClusterStateChangeEvent objects are passed to enforcePolicy before they reach the VHM event queue
 */
public interface PolicyMonitor {

   /**
    * When a cluster state change occurs, enforcePolicy is invoked with the event describing the change
    * The method should not modify the event passed in
    * @PolicyViolationEvent events returned are placed on the VHM event queue along with the @ClusterStateChangeEvent object
    * 
    * @param csce A ClusterStateChangeEvent before it reaches the VHM queue
    * @return A list of PolicyViolationEvents or null
    */
   List<PolicyViolationEvent> enforcePolicy(final ClusterStateChangeEvent csce);

}
