package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public abstract class AbstractClusterScaleDecision extends AbstractNotificationEvent implements ClusterScaleCompletionEvent {

   String _clusterId;
   
   public AbstractClusterScaleDecision(String clusterId) {
      super(false, false);
      _clusterId = clusterId;
   }

   @Override
   public String getClusterId() {
      return _clusterId;
   }

}
