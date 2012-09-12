package com.vmware.vhadoop.vhm;

public class VHMSimpleInputMessage implements EmbeddedVHM.VHMInputMessage {
   
   private byte[] _data;
   private String _clusterName;
   private int _targetTTs;
   private String _jobTracker;
   private String _ttFolderName;
   
   public VHMSimpleInputMessage(byte[] data) {
      _data = data;
      String stringData = new String(data);
      String delims = "[ ]+";
      String[] tokens = stringData.split(delims);
      _clusterName = tokens[0];
      _targetTTs = Integer.parseInt(tokens[2]);
      _jobTracker = tokens[3];
      _ttFolderName = tokens[4];
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
   /* TODO: return a one element array to work around future requirement */
   public String[] getTTFolderNames() {
      return new String[]{_ttFolderName};
   }

}
