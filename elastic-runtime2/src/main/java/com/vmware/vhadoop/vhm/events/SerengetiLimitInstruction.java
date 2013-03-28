package com.vmware.vhadoop.vhm.events;

public class SerengetiLimitInstruction extends AbstractClusterScaleEvent {
   String _clusterFolderName;
   int _toSize;

   public SerengetiLimitInstruction(String clusterFolderName, int toSize) {
      super(false, false);
      _clusterFolderName = clusterFolderName;
      _toSize = toSize;
   }

   public String getClusterFolderName() {
      return _clusterFolderName;
   }
   
   public int getToSize() {
      return _toSize;
   }
}
