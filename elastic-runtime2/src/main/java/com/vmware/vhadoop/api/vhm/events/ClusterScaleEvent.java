package com.vmware.vhadoop.api.vhm.events;

public interface ClusterScaleEvent extends NotificationEvent {

   String getVmId();
   
   String getHostId();
   
   String getClusterId();
}
