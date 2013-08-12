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

import java.util.Set;

/* Represents actions which can be invoked on the Hadoop subsystem */
public interface HadoopActions {

   public class HadoopClusterInfo {
      private String _clusterId;
      private String _jobTrackerDnsName;
      private String _jobTrackerIpAddr;
      private Integer _jobTrackerPort;
      
      public HadoopClusterInfo(String clusterId, String jobTrackerDnsName, String jobTrackerIpAddr, Integer jobTrackerPort) {
         _clusterId = clusterId;
         _jobTrackerDnsName = jobTrackerDnsName;
         _jobTrackerIpAddr = jobTrackerIpAddr;
         _jobTrackerPort = jobTrackerPort;
      }
      
      public String getClusterId() {
         return _clusterId;
      }
      
      public String getJobTrackerDnsName() {
         return _jobTrackerDnsName;
      }

      public String getJobTrackerIpAddr() {
         return _jobTrackerIpAddr;
      }

      public Integer getJobTrackerPort() {
         return _jobTrackerPort;
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

   public void decommissionTTs(Set<String> ttDnsNames, HadoopClusterInfo cluster);

   public void recommissionTTs(Set<String> ttDnsNames, HadoopClusterInfo cluster);
	
   public Set<String> checkTargetTTsSuccess(String opType, Set<String> ttDnsNames, int totalTargetEnabled, HadoopClusterInfo cluster);
	
   public Set<String> getActiveTTs(HadoopClusterInfo cluster, int totalTargetEnabled);
}
