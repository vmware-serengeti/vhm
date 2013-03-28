package com.vmware.vhadoop.vhm.events;

public class ClusterScaleDecisionExpand extends AbstractClusterScaleDecision {

   int _fromNodes, _toNodes;
   
   public ClusterScaleDecisionExpand(String clusterId, int fromNodes, int toNodes) {
      super(clusterId);
      _fromNodes = fromNodes;
      _toNodes = toNodes;
   }
   
   public int getFromNodes() {
      return _fromNodes;
   }

   public int getToNodes() {
      return _toNodes;
   }

}
