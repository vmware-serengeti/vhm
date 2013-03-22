package com.vmware.vhadoop.vhm.events;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;

public class SerengetiLimitEvent extends NotificationEvent implements ClusterScaleEvent {
   String _clusterFolderName;
   int _toSize;

   public SerengetiLimitEvent(String clusterFolderName, int toSize) {
      super(false, false);
      _clusterFolderName = clusterFolderName;
      _toSize = toSize;
   }

   @Override
   public String getClusterFolderName() {
      return _clusterFolderName;
   }
   
   public int getToSize() {
      return _toSize;
   }
}
