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

package com.vmware.vhadoop.external;

public interface HadoopActions {

   /** 
	 * 	Decommission a given set of TaskTrackers from a JobTracker 
	 *	@param TaskTrackers that need to be decommissioned
	 *  @return SUCCESS/FAIL
	 */
		public boolean decommissionTTs(String[] tts, HadoopCluster cluster);

	/** 
	 * 	Recommission a given set of TaskTrackers to a JobTracker 
	 *	@param TaskTrackers that need to be recommissioned/added
	 *  @return SUCCESS/FAIL
	 */
		public boolean recommissionTTs(String[] tts, HadoopCluster cluster);

		
		public boolean checkTargetTTsSuccess(String opType, String[] tts, int totalTargetEnabled, HadoopCluster cluster);
}
