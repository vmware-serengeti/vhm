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

   public TrivialClusterScaleEvent(String vmId, String hostId, String clusterId, String routeKey, ChannelReporter reporter) {
      _vmId = vmId;
      _hostId = hostId;
      _clusterId = clusterId;
      _routeKey = routeKey;
      _reporter = reporter;
   }
   
   public interface ChannelReporter {
      public void reportBack(String routeKey);
   }
   
   public TrivialClusterScaleEvent(String clusterId) {
      this(null, null, clusterId, null, null);
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
}
