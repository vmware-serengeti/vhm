package com.vmware.vhadoop.api.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;

/* Defines a simple abstraction for handling cluster scale events in a multi-threaded way */
public interface ExecutionStrategy {

   boolean handleClusterScaleEvents(String clusterId, ScaleStrategy scaleStrategy, Set<ClusterScaleEvent> events);

   boolean isClusterScaleInProgress(String clusterId);

}
