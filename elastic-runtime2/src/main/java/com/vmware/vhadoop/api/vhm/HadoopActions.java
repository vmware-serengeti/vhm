/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
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

import com.vmware.vhadoop.util.CompoundStatus;

public interface HadoopActions {

   public class HadoopClusterInfo {
      private String _clusterId;
      private String _jobTrackerName;
      
      public HadoopClusterInfo(String clusterId, String jobTrackerName) {
         _clusterId = clusterId;
         _jobTrackerName = jobTrackerName;
      }
      
      public String getClusterId() {
         return _clusterId;
      }
      
      public String getJobTrackerName() {
         return _jobTrackerName;
      }
   }

   public class JTConfigInfo {
      String _hadoopHomePath;
      String _excludeTTPath;

      public JTConfigInfo(String hadoopHomePath, String excludeTTPath) {
         _hadoopHomePath = hadoopHomePath;
         _excludeTTPath = excludeTTPath;
      }
      
      public String getHadoopHomePath() {
         return _hadoopHomePath;
      }
      
      public String getExcludeTTPath() {
         return _excludeTTPath;
      }
   }

   /** 
	 * 	Decommission a given set of TaskTrackers from a JobTracker 
	 *	@param TaskTrackers that need to be decommissioned
	 *  @return SUCCESS/FAIL
	 */
		public CompoundStatus decommissionTTs(String[] tts, HadoopClusterInfo cluster);

	/** 
	 * 	Recommission a given set of TaskTrackers to a JobTracker 
	 *	@param TaskTrackers that need to be recommissioned/added
	 *  @return SUCCESS/FAIL
	 */
		public CompoundStatus recommissionTTs(String[] tts, HadoopClusterInfo cluster);

		
		public CompoundStatus checkTargetTTsSuccess(String opType, String[] tts, int totalTargetEnabled, HadoopClusterInfo cluster);
}
