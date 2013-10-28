package com.vmware.vhadoop.api.vhm;

import java.util.List;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.PolicyViolationEvent;

public interface PolicyMonitor {

   List<PolicyViolationEvent> enforcePolicy(ClusterStateChangeEvent csce);

}
