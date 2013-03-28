package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;

public class AbstractClusterScaleEvent extends AbstractNotificationEvent
      implements ClusterScaleEvent {

   private String _vmId;
   private String _hostId;
   private String _clusterId;
   
   public AbstractClusterScaleEvent() {
      super(false, false);
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
   
   public void setVmId(String vmId) {
      _vmId = vmId;
   }

   public void setClusterId(String clusterId) {
      _clusterId = clusterId;
   }

   public void setHostId(String hostId) {
      _hostId = hostId;
   }
}
