package com.vmware.vhadoop.api.vhm;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;

public interface ExecutionStrategy {

   void handleClusterScaleEvents(ScaleStrategy scaleStrategy, Set<ClusterScaleEvent> events);

}
