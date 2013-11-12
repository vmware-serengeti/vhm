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

import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;

public abstract class AbstractClusterScaleEvent extends AbstractNotificationEvent
      implements ClusterScaleEvent {

   private String _vmId;
   private String _hostId;
   private String _clusterId;
   private String _reason;

   public AbstractClusterScaleEvent(String reason) {
      super(false, true);

      _reason = reason;
   }

   @Override
   public String getVmId() {
      return _vmId;
   }

   @Override
   public String getHostId() {
      return _hostId;
   }

   @Override
   public String getClusterId() {
      return _clusterId;
   }

   @Override
   public String getReason() {
      return _reason;
   }

   public void setVmId(String vmId) {
      _vmId = vmId;
   }

   public void setClusterId(String clusterId) {
      _clusterId = clusterId;
   }

   public void setHostId(String hostId) {
      _hostId = hostId;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_clusterId == null) ? 0 : _clusterId.hashCode());
      result = prime * result + ((_hostId == null) ? 0 : _hostId.hashCode());
      result = prime * result + ((_vmId == null) ? 0 : _vmId.hashCode());
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
      AbstractClusterScaleEvent other = (AbstractClusterScaleEvent) obj;
      if (_clusterId == null) {
         if (other._clusterId != null)
            return false;
      } else if (!_clusterId.equals(other._clusterId))
         return false;
      if (_hostId == null) {
         if (other._hostId != null)
            return false;
      } else if (!_hostId.equals(other._hostId))
         return false;
      if (_vmId == null) {
         if (other._vmId != null)
            return false;
      } else if (!_vmId.equals(other._vmId))
         return false;
      return true;
   }
   
   @Override
   public String toString() {
      return this.getClass().getSimpleName()+
            ((_clusterId != null) ? " clusterId=<%C"+_clusterId+"%C>, " : "")+
            ((_vmId != null) ? " vmId=<%C"+_vmId+"%C>, " : "")+
            ((_hostId != null) ? " hostId="+_hostId+"," : "")+
            ((_reason != null) ? " reason=\""+_reason+"\"" : "");
            
   }
}
