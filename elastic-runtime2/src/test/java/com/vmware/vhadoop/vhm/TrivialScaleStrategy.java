/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

package com.vmware.vhadoop.vhm;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategyContext;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;

public class TrivialScaleStrategy extends AbstractClusterMapReader implements ScaleStrategy {
   String _testKey;
   Map<String, ClusterScaleOperation> _clusterScaleOperations;
   TrivialClusterScaleOperation _trivialClusterScaleOperation;
 
   private static final Logger _log = Logger.getLogger(TrivialScaleStrategy.class.getName());

   public class TrivialClusterScaleOperation extends ClusterScaleOperation {
      String _clusterId;
      Set<ClusterScaleEvent> _events;
      ScaleStrategyContext _context;
      long _scalePauseMillis = 0;
      int _requeueEventTimes = 0;
      
      public TrivialClusterScaleOperation() {
         initialize(TrivialScaleStrategy.this);
      }
      
      public TrivialClusterScaleOperation(long pauseDuringScaleMillis) {
         _scalePauseMillis = pauseDuringScaleMillis;
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
      
      void setRequeueEventTimes(int numTimes) {
         _requeueEventTimes = numTimes;
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
         _log.info("About to scale cluster "+_clusterId);
         Thread.sleep(_scalePauseMillis);
         _log.info("Done scaling cluster "+_clusterId);
         TrivialClusterScaleEvent tcse = (TrivialClusterScaleEvent)_events.iterator().next();
         tcse.ReportBack();
         ClusterScaleDecision result = new ClusterScaleDecision(_clusterId);
         if (_requeueEventTimes-- > 0) {
            for (NotificationEvent event : _events) {
               result.requeueEventForCluster(event);
            }
         }
         return result;
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

   @SuppressWarnings("unchecked")
   @Override
   public Class<? extends ClusterScaleEvent>[] getScaleEventTypesHandled() {
      return new Class[]{ClusterScaleEvent.class};       /* Handle all events */
   }

}
