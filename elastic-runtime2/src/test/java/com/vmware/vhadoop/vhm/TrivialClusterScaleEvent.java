package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;

public class TrivialClusterScaleEvent extends AbstractClusterScaleEvent {
   String _vmId = null;
   String _hostId = null;
   String _clusterId = null;

   public TrivialClusterScaleEvent(String vmId, String hostId, String clusterId) {
      _vmId = vmId;
      _hostId = hostId;
      _clusterId = clusterId;
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
}
