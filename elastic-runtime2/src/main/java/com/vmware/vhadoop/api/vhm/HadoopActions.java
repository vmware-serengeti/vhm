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

import java.util.Set;

/* Represents actions which can be invoked on the Hadoop subsystem */
public interface HadoopActions {

   public class HadoopClusterInfo {
      private String _clusterId;
      private String _jobTrackerDnsName;
      private Integer _jobTrackerPort;
      
      public HadoopClusterInfo(String clusterId, String jobTrackerDnsName, Integer jobTrackerPort) {
         _clusterId = clusterId;
         _jobTrackerDnsName = jobTrackerDnsName;
         _jobTrackerPort = jobTrackerPort;
      }
      
      public String getClusterId() {
         return _clusterId;
      }
      
      public String getJobTrackerDnsName() {
         return _jobTrackerDnsName;
      }

      public Integer getJobTrackerPort() {
         return _jobTrackerPort;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((_clusterId == null) ? 0 : _clusterId.hashCode());
         result = prime * result + ((_jobTrackerDnsName == null) ? 0 : _jobTrackerDnsName.hashCode());
         result = prime * result + ((_jobTrackerPort == null) ? 0 : _jobTrackerPort.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         HadoopClusterInfo other = (HadoopClusterInfo) obj;
         if (_clusterId == null) {
            if (other._clusterId != null)
               return false;
         } else if (!_clusterId.equals(other._clusterId))
            return false;
         if (_jobTrackerDnsName == null) {
            if (other._jobTrackerDnsName != null)
               return false;
         } else if (!_jobTrackerDnsName.equals(other._jobTrackerDnsName))
            return false;
         if (_jobTrackerPort == null) {
            if (other._jobTrackerPort != null)
               return false;
         } else if (!_jobTrackerPort.equals(other._jobTrackerPort))
            return false;
         return true;
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

   public boolean validateTtHostNames(Set<String> dnsNames);
}
