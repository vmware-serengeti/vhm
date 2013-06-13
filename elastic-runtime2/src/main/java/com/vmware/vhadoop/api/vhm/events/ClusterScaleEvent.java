package com.vmware.vhadoop.api.vhm.events;

/* A ClusterScaleEvent represents a request to scale a particular cluster, based on data from either a VM, a Host or a Cluster */
public interface ClusterScaleEvent extends NotificationEvent {

   String getVmId();
   
   String getHostId();
   
   String getClusterId();
   
   boolean isExclusive();
}
