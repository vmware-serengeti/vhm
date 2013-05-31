package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;

public class ClusterUpdateEvent extends AbstractNotificationEvent implements ClusterStateChangeEvent {
   private String _vmId;
   private SerengetiClusterVariableData _clusterVariableData;
   
   public ClusterUpdateEvent(String vmId, SerengetiClusterVariableData clusterVariableData) {
      super(false, false);
      _vmId = vmId;
      _clusterVariableData = clusterVariableData;
   }
   
   public String getVmId() {
      return _vmId;
   }

   public SerengetiClusterVariableData getClusterVariableData() {
      return _clusterVariableData;
   }
}
