package com.vmware.vhadoop.external;

public class HadoopCluster {
   private String _clusterName;
   private String _jobTrackerName;
   
   public HadoopCluster(String clusterName, String jobTrackerName) {
      _clusterName = clusterName;
      _jobTrackerName = jobTrackerName;
   }
   
   public String getClusterName() {
      return _clusterName;
   }
   
   public String getJobTrackerName() {
      return _jobTrackerName;
   }
}
