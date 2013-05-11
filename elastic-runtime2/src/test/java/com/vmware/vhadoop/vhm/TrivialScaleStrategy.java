package com.vmware.vhadoop.vhm;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;

public class TrivialScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   String _testKey;
   Map<String, ClusterScaleOperation> _clusterScaleOperations;
   TrivialClusterScaleOperation _trivialClusterScaleOperation;
   
   public class TrivialClusterScaleOperation extends ClusterScaleOperation {
      String _clusterId;
      Set<ClusterScaleEvent> _events;
      ScaleStrategyContext _context;
      
      public TrivialClusterScaleOperation() {
         initialize(TrivialScaleStrategy.this);
      }
      
      void setClusterId(String clusterId) {
         _clusterId = clusterId;
      }
      
      void setEvents(Set<ClusterScaleEvent> events) {
         _events = events;
      }
      
      void setContext(ScaleStrategyContext context) {
         _context = context;
      }
      
      public String getClusterId() {
         return _clusterId;
      }
      
      public Set<ClusterScaleEvent> getEvents() {
         return _events;
      }
      
      public ScaleStrategyContext getContext() {
         return _context;
      }

      @Override
      public ClusterScaleCompletionEvent localCall() throws Exception {
         return new ClusterScaleDecision(_clusterId);
      }
   }
   
   public TrivialScaleStrategy(String testKey) {
      _testKey = testKey;
      _clusterScaleOperations = new HashMap<String, ClusterScaleOperation>();
   }

   @Override
   public String getKey() {
      return _testKey;
   }

   /* If it's not static, it can't be created reflectively */
   public static class TrivialStrategyContext implements ScaleStrategyContext {
      private String _someContextInfo;

      public String getSomeContextInfo() {
         return _someContextInfo;
      }

      public void setSomeContextInfo(String someContextInfo) {
         _someContextInfo = someContextInfo;
      }
   }
   
   @Override
   public Class<? extends ScaleStrategyContext> getStrategyContextType() {
      return TrivialStrategyContext.class;
   }

   void setClusterScaleOperation(String clusterId, ClusterScaleOperation cso) {
      _clusterScaleOperations.put(clusterId, cso);
   }
   
   @Override
   public ClusterScaleOperation getClusterScaleOperation(String clusterId,
         Set<ClusterScaleEvent> events, ScaleStrategyContext context) {
      ClusterScaleOperation returnVal = _clusterScaleOperations.get(clusterId);
      if (returnVal == null) {
         returnVal = new TrivialClusterScaleOperation();
      }
      if (returnVal instanceof TrivialClusterScaleOperation) {
         ((TrivialClusterScaleOperation)returnVal).setClusterId(clusterId);
         ((TrivialClusterScaleOperation)returnVal).setContext(context);
         ((TrivialClusterScaleOperation)returnVal).setEvents(events);
      }
      return returnVal;
   }

}
