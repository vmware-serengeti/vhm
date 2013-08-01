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
   String _vmId = null;
   String _hostId = null;
   String _clusterId = null;
   String _routeKey = null;
   ChannelReporter _reporter = null;
   private static int cntr = 0;
   private int _id = ++cntr;
   private boolean _isExclusive;

   public TrivialClusterScaleEvent(String vmId, String hostId, String clusterId, String routeKey, ChannelReporter reporter) {
      super("trivial cluster scale event (test)");
      _vmId = vmId;
      _hostId = hostId;
      _clusterId = clusterId;
      _routeKey = routeKey;
      _reporter = reporter;
   }

   public interface ChannelReporter {
      public void reportBack(String routeKey);
   }

   public TrivialClusterScaleEvent(String clusterId, boolean isExclusive) {
      this(null, null, clusterId, null, null);
      _isExclusive = isExclusive;
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
}
