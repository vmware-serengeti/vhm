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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;

public class ClusterScaleDecision extends AbstractNotificationEvent implements ClusterScaleCompletionEvent {
   final String _clusterId;
   Map<String, Decision> _decisions;
   List<NotificationEvent> _eventsToRequeue;
   
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
      if (vmIds != null) {
         for (String vmId : vmIds) {
            _decisions.put(vmId, decision);
         }
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

   @Override
   public ClusterScaleCompletionEvent immutableCopy() {
      ClusterScaleDecision copy = new ClusterScaleDecision(_clusterId);
      copy._decisions = (_decisions == null) ? null : Collections.unmodifiableMap(_decisions);
      copy._eventsToRequeue = (_eventsToRequeue == null) ? null : Collections.unmodifiableList(_eventsToRequeue);
      return copy;
   }
   
   @Override
   public String toString() {
      return "ClusterScaleDecision{clusterId=<%C"+_clusterId+"%C>, decisions="+_decisions+", eventsToRequeue="+_eventsToRequeue+"}";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_clusterId == null) ? 0 : _clusterId.hashCode());
      result = prime * result + ((_decisions == null) ? 0 : _decisions.hashCode());
      result = prime * result + ((_eventsToRequeue == null) ? 0 : _eventsToRequeue.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ClusterScaleDecision other = (ClusterScaleDecision) obj;
      if (_clusterId == null) {
         if (other._clusterId != null)
            return false;
      } else if (!_clusterId.equals(other._clusterId))
         return false;
      if (_decisions == null) {
         if (other._decisions != null)
            return false;
      } else if (!_decisions.equals(other._decisions))
         return false;
      if (_eventsToRequeue == null) {
         if (other._eventsToRequeue != null)
            return false;
      } else if (!_eventsToRequeue.equals(other._eventsToRequeue))
         return false;
      return true;
   }
}
