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

package com.vmware.vhadoop.api.vhm;

/* Any class wanting to access ClusterMap data should implement ClusterMapReader
 * Easiest way to do this is to extend AbstractClusterMapReader.
 * ClusterMapReader provides a simple multiple-reader single-writer locking mechanism via the ClusterMapAccess interface
 * A ClusterMapReader can only be initialized with a reference to another ClusterMapReader - in this way, one reader can hand access to others
 */
public interface ClusterMapReader {
   
   public static final String POWER_STATE_CHANGE_STATUS_KEY = "blockOnPowerStateChange";

   public interface ClusterMapAccess {
      ClusterMap lockClusterMap();

      boolean unlockClusterMap(ClusterMap clusterMap);
   }
      
   void initialize(ClusterMapReader parent);
   
   /* Holding a read lock on ClusterMap ensures that it won't change until unlockClusterMap is called */
   ClusterMap getAndReadLockClusterMap();

   void unlockClusterMap(ClusterMap clusterMap);
}
