package com.vmware.vhadoop.vhm;

public class VHMSimpleInputMessage implements EmbeddedVHM.VHMInputMessage {
   
   private byte[] _data;
   private String _clusterName;
   private int _targetTTs;
   private String _jobTracker;
   private String[] _ttFolderNames;
   
   public VHMSimpleInputMessage(byte[] data) {
      _data = data;
      String stringData = new String(data);
      String delims = "[ ]+";
      String[] tokens = stringData.split(delims);
      _clusterName = tokens[0];
      _targetTTs = Integer.parseInt(tokens[2]);
      _jobTracker = tokens[3];
      _ttFolderNames = tokens[4].split(",");
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
