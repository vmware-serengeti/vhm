package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMInputMessage;

public class VHMJsonInputMessage implements VHMInputMessage {

   private byte[] _data;
   private String _clusterName;
   private int _targetTTs;
   private String _jobTracker;
   private String[] _ttFolderNames;
   
   public VHMJsonInputMessage(byte[] data) {
      /* TODO: Process JSON data and set the fields */
   }

   @Override
   public byte[] getRawPayload() {
      return _data;
   }
   
   @Override
   public String getClusterName() {
      return _clusterName;
   }
   
   @Override
   public int getTargetTTs() {
      return _targetTTs;
   }

   @Override
   public String getJobTrackerAddress() {
      return _jobTracker;
   }

   @Override
   public String[] getTTFolderNames() {
      return _ttFolderNames;
   }

}
