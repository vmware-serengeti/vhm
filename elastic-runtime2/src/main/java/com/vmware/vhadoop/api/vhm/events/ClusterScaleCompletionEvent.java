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

package com.vmware.vhadoop.api.vhm.events;

/* An event generated once a cluster scale operation has completed representing Decisions for a given VM */
public interface ClusterScaleCompletionEvent extends NotificationEvent {

   /* No such thing as an extensible enumeration */
   public static final class Decision {
      String _value;
      public Decision(String name) {
         _value = name;
      }
      @Override
      public String toString() {
         return _value;
      }
   }

   public static final Decision ENABLE = new Decision("ENABLE");
   public static final Decision DISABLE = new Decision("DISABLE");
      
   String getClusterId();
   
   Decision getDecisionForVM(String vmId);
}
