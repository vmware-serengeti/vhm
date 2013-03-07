package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;

public class SerengetiLimitEvent extends NotificationEvent implements ClusterScaleEvent {
   String _clusterId;
   int _fromSize;
   int _toSize;

   public SerengetiLimitEvent(String clusterId, int fromSize, int toSize) {
      super(false, false);
      _clusterId = clusterId;
      _fromSize = fromSize;
      _toSize = toSize;
   }

   @Override
   public String getClusterId() {
      return _clusterId;
   }

   public int getFromSize() {
      return _fromSize;
   }

   public int getToSize() {
      return _toSize;
   }
}
