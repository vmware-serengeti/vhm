package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;

public class ScaleStrategyChangeEvent extends NotificationEvent implements ClusterStateChangeEvent{
   private String _newStrategyKey;
   private String _clusterId;
   
   public ScaleStrategyChangeEvent(String clusterId, String newStrategyKey) {
      super(false, false);
      _clusterId = clusterId;
      _newStrategyKey = newStrategyKey;
   }
   
   public String getNewStrategyKey() {
      return _newStrategyKey;
   }
   
   public String getClusterId() {
      return _clusterId;
   }
}
