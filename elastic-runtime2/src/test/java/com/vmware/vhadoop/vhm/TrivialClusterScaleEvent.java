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

package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;

public class TrivialClusterScaleEvent extends AbstractClusterScaleEvent {
   String _routeKey = null;
   ChannelReporter _reporter = null;
   private static int cntr = 0;
   private int _id = ++cntr;
   private boolean _isExclusive;
   private Long _fixedTimestamp;

   public TrivialClusterScaleEvent(String vmId, String hostId, String clusterId, String routeKey, ChannelReporter reporter, boolean isExclusive) {
      super("trivial cluster scale event (test)");
      setVmId(vmId);
      setHostId(hostId);
      setClusterId(clusterId);
      _routeKey = routeKey;
      _reporter = reporter;
      _isExclusive = isExclusive;
   }

   public interface ChannelReporter {
      public void reportBack(String routeKey);
   }

   public TrivialClusterScaleEvent(String clusterId, boolean isExclusive) {
      this(null, null, clusterId, null, null, isExclusive);
   }

   public TrivialClusterScaleEvent(String clusterId, boolean isExclusive, long fixTimestamp) {
      this(null, null, clusterId, null, null, isExclusive);
      _fixedTimestamp = fixTimestamp;
   }

   public TrivialClusterScaleEvent(String vmId, String hostId, String clusterId, String routeKey, 
         ChannelReporter reporter, boolean isExclusive, long fixTimestamp) {
      this(vmId, hostId, clusterId, routeKey, reporter, isExclusive);
      _fixedTimestamp = fixTimestamp;
   }
   
   public void ReportBack() {
      if (_reporter != null) {
         _reporter.reportBack(_routeKey);
      }
   }

   @Override
   public String toString() {
      return "TrivialClusterScaleEvent"+_id;
   }

   @Override
   public boolean isExclusive() {
      return _isExclusive;
   }
   
   @Override
   public long getTimestamp() {
      if (_fixedTimestamp == null) {
         return super.getTimestamp();
      }
      return _fixedTimestamp;
   }
}
