package com.vmware.vhadoop.api.vhm.events;


public interface ClusterScaleEvent extends NotificationEvent {

   String getClusterId();
}
