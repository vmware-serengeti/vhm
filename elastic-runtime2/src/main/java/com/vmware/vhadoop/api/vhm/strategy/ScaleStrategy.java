package com.vmware.vhadoop.api.vhm.strategy;

import java.util.Set;
import java.util.concurrent.Callable;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;

public interface ScaleStrategy extends ClusterMapReader {

   Callable getCallable(Set<ClusterScaleEvent> events);
}
