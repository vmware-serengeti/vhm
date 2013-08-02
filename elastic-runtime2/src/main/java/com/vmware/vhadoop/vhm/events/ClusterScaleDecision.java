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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;

public class ClusterScaleDecision extends AbstractNotificationEvent implements ClusterScaleCompletionEvent {
   private final String _clusterId;
   private final Map<String, Decision> _decisions;
   private List<NotificationEvent> _eventsToRequeue;
   
   public ClusterScaleDecision(String clusterId) {
      super(false, true);
      _clusterId = clusterId;
      _decisions = new HashMap<String, Decision>();
   }

   @Override
   public String getClusterId() {
      return _clusterId;
   }

   public void addDecision(String vmId, Decision decision) {
      _decisions.put(vmId, decision);
   }

   public void addDecision(Set<String> vmIds, Decision decision) {
      for (String vmId : vmIds) {
         _decisions.put(vmId, decision);
      }
   }

   @Override
   public Decision getDecisionForVM(String vmId) {
      return _decisions.get(vmId);
   }

   @Override
   public Set<String> getVMsForDecision(Decision decision) {
      Set<String> vms = null;
      for (String vm : _decisions.keySet()) {
         if (_decisions.get(vm).equals(decision)) {
            if (vms == null) {
               vms = new HashSet<String>();
            }
            vms.add(vm);
         }
      }

      return vms;
   }

   @Override
   public void requeueEventForCluster(NotificationEvent event) {
      if (_eventsToRequeue == null) {
         _eventsToRequeue = new ArrayList<NotificationEvent>();
      }
      _eventsToRequeue.add(event);
   }
   
   public List<NotificationEvent> getEventsToRequeue() {
      return _eventsToRequeue;
   }
}
