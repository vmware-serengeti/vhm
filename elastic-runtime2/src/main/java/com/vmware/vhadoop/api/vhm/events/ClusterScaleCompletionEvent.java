package com.vmware.vhadoop.api.vhm.events;

public interface ClusterScaleCompletionEvent extends NotificationEvent {

   String getClusterId();

}
