package com.vmware.vhadoop.api.vhm;

import java.util.concurrent.Callable;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;

public interface ScaleStrategy extends ClusterMapReader {

   Callable getCallable(ClusterScaleEvent event);
}
