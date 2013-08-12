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

package com.vmware.vhadoop.vhm.strategy;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vhadoop.vhm.AbstractClusterMapReader;

public class JobTrackerEDPolicy extends AbstractClusterMapReader implements EDPolicy {
   private static final Logger _log = Logger.getLogger(JobTrackerEDPolicy.class.getName());

   private final HadoopActions _hadoopActions;
   private final VCActions _vcActions;

   private static final long MAX_DNS_WAIT_TIME_MILLIS = 120000;
   
   public JobTrackerEDPolicy(HadoopActions hadoopActions, VCActions vcActions) {
      _hadoopActions = hadoopActions;
      _vcActions = vcActions;
   }

   /* This method blocks until it has made all reasonable efforts to determine that the TTs have been successfully registered with the JT 
    * 
    * Hadoop re-commission must work with dnsNames, whereas the power-on needs VM IDs. If TTs have been shut down in a certain way or if
    * there are error conditions, such as dns problems, the TTs may not have dnsNames. In the former case, DNS names will be discovered
    * after power-on and can be used to verify re-commission status. In the latter, the method will timeout trying to retrieve dns names 
    * 
    * Another issue is that if VMs are hard-decommissioned (via power off) and then re-commissioned, JT will think they are still alive and
    * immediately pass the re-commission check. Worst case is that the method exits too early. */
   @Override
   public Set<String> enableTTs(Set<String> ttVmIds, int totalTargetEnabled, String clusterId) throws Exception {
      Map<String, String> dnsNameMap = null;
      HadoopClusterInfo hadoopCluster = null;
      Set<String> activeVmIds = null;
 
      ClusterMap clusterMap = null;
      try {
         clusterMap = getAndReadLockClusterMap();
         hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
         dnsNameMap = clusterMap.getDnsNamesForVMs(ttVmIds);
      } finally {
         unlockClusterMap(clusterMap);
      }

      /* Note: Individual DNS name values can be null here in some circumstances, but the valid vmIds will still be part of the hostNames keySet() */
      if ((dnsNameMap != null) && (hadoopCluster != null) && (hadoopCluster.getJobTrackerIpAddr() != null)) {
         CompoundStatus status = getCompoundStatus();

         Set<String> validDnsNames = getValidDnsNames(dnsNameMap);
         Set<String> vmIdsWithInvalidDns = getVmIdsWithInvalidDnsNames(dnsNameMap);

         _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>"+constructUserLogMessage(vmIdsWithInvalidDns, validDnsNames, false));

         /* Only send TTs with valid dnsNames to be properly recommissioned - the rest will just be powered on */
         if (validDnsNames != null) {
            _hadoopActions.recommissionTTs(validDnsNames, hadoopCluster);
         }
         if (_vcActions.changeVMPowerState(ttVmIds, true) == null) {
            status.registerTaskFailed(false, "Failed to change VM power state in VC");
            _log.log(VhmLevel.USER, "<%C"+clusterId+"%C> Failed to power on Task Trackers");
         } else {
            if (status.screenStatusesForSpecificFailures(new String[]{VCActions.VC_POWER_ON_STATUS_KEY})) {
               if (vmIdsWithInvalidDns != null) {
                  Set<String> newDnsNames = blockAndGetDnsNamesForVmIdsWithoutCachedDns(vmIdsWithInvalidDns, MAX_DNS_WAIT_TIME_MILLIS);
                  if (newDnsNames != null) {
                     validDnsNames.addAll(newDnsNames);
                  }
               }
               /* Even if we still don't have a full list of valid DNS names, we have to hope we'll reach our original target */
               activeVmIds = _hadoopActions.checkTargetTTsSuccess("Recommission", validDnsNames, totalTargetEnabled, hadoopCluster);
            } else {
               _log.log(VhmLevel.USER, "<%C"+clusterId+"%C> Unexpected VC error powering on Task Trackers");
            }
         }
      }
      return activeVmIds;
   }

   /* This method blocks until it has made all reasonable efforts to determine that the TTs have been successfully unregistered with the JT 
    * 
    * Effective hadoop de-commission must work with dnsNames, whereas the power-off needs VM IDs. In certain error cases, there may be no
    * valid DNS name for some of the vmIds to de-commission. In this case, we must simply power those off. Note that this may leave the JT
    * thinking that these TTs are still alive for a period of time */
   @Override
   public Set<String> disableTTs(Set<String> ttVmIds, int totalTargetEnabled, String clusterId) throws Exception {
      Map<String, String> dnsNameMap = null;
      HadoopClusterInfo hadoopCluster = null;
      Set<String> activeVmIds = null;

      ClusterMap clusterMap = null;
      try {
         clusterMap = getAndReadLockClusterMap();
         hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
         dnsNameMap = clusterMap.getDnsNamesForVMs(ttVmIds);
      } finally {
         unlockClusterMap(clusterMap);
      }

      if ((dnsNameMap != null) && (hadoopCluster != null) && (hadoopCluster.getJobTrackerIpAddr() != null)) {
         CompoundStatus status = getCompoundStatus();
         
         Set<String> validDnsNames = getValidDnsNames(dnsNameMap);
         Set<String> vmIdsWithInvalidDns = getVmIdsWithInvalidDnsNames(dnsNameMap);
         /* Since we can only check for de-commission of VMs with valid dns names, we should adjust the target accordingly */
         int newTargetEnabled = (vmIdsWithInvalidDns == null) ? totalTargetEnabled : totalTargetEnabled + vmIdsWithInvalidDns.size();

         _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>"+constructUserLogMessage(vmIdsWithInvalidDns, validDnsNames, true));

         /* Only send TTs with valid dnsNames to be properly decommissioned - the rest will just be powered off */
         if (validDnsNames != null) {
            _hadoopActions.decommissionTTs(validDnsNames, hadoopCluster);
         }
         /* TODO: Legacy code returns a CompoundStatus rather than modifying thread local version. Ideally it would be refactored for consistency */
         if (status.screenStatusesForSpecificFailures(new String[]{"decomRecomTTs"})) {
            activeVmIds = getActiveVmIds(_hadoopActions.checkTargetTTsSuccess("Decommission", validDnsNames, newTargetEnabled, hadoopCluster));
            if ((activeVmIds != null) && !activeVmIds.isEmpty()) {
               _log.log(VhmLevel.USER, "The following task trackers failed to decommission cleanly: "+LogFormatter.constructListOfLoggableVms(activeVmIds));
            }
         }
         /* Power off all the VMs, decommissioned or not - note this does not block */
         if (_vcActions.changeVMPowerState(ttVmIds, false) == null) {
            status.registerTaskFailed(false, "Failed to change VM power state in VC");
            _log.log(VhmLevel.USER, "<%C"+clusterId+"%C> Unexpected VC error powering off Task Trackers");
         }
      }
      return getSuccessfullyDisabledVmIds(ttVmIds, activeVmIds);
   }
   
   private Set<String> getVmIdsWithInvalidDnsNames(Map<String, String> dnsNameMap) {
      Set<String> vmIdsWithInvalidDnsNames = null;
      for (String ttVmId : dnsNameMap.keySet()) {
         String dnsName = dnsNameMap.get(ttVmId);
         if ((dnsName == null) || (dnsName.trim().length() == 0)) {
            if (vmIdsWithInvalidDnsNames == null) {
               vmIdsWithInvalidDnsNames = new HashSet<String>();
            }
            vmIdsWithInvalidDnsNames.add(ttVmId);
         }
      }
      return vmIdsWithInvalidDnsNames;
   }

   private Set<String> blockAndGetDnsNamesForVmIdsWithoutCachedDns(Set<String> vmIdsWithInvalidDns, long timeoutMillis) {
      long endTime = System.currentTimeMillis() + timeoutMillis;
      Set<String> result = null;
      if (vmIdsWithInvalidDns != null) {
         do {
            ClusterMap clusterMap = null;
            Map<String, String> newDnsNameMap = null;
            try {
               clusterMap = getAndReadLockClusterMap();
               newDnsNameMap = clusterMap.getDnsNamesForVMs(vmIdsWithInvalidDns);
               if (newDnsNameMap == null) {
                  return null;         /* This would mean that our vmIds themselves have become invalid, which would only occur if vms are deleted */
               }
               result = new HashSet<String>(newDnsNameMap.values());
               if (!result.contains(null) && !result.contains("")) {
                  _log.info("Found valid DNS names for all VMs");
                  return result;
               }
            } finally {
               unlockClusterMap(clusterMap);
            }
            _log.info("Still looking for valid DNS names for "+LogFormatter.constructListOfLoggableVms(getVmIdsWithInvalidDnsNames(newDnsNameMap)));
            try {
               Thread.sleep(5000);
            } catch (InterruptedException e) {}
         } while (System.currentTimeMillis() <= endTime);
         /* If we fell out of the loop, it's likely we didn't find everything we were looking for */
         if (result != null) {
            result.remove(null);
            result.remove("");
            if (result.isEmpty()) {
               result = null;
            }
         }
      }
      return result;
   }

   private String constructUserLogMessage(Set<String> vmIdsWithInvalidDns, Set<String> validDnsNames, boolean isDecommission) {
      int toDeRecommission = (validDnsNames == null) ? 0 : (validDnsNames.size());
      int toChangePowerState = (vmIdsWithInvalidDns == null) ? 0 : (vmIdsWithInvalidDns.size());
      String powerOffMsg = (toChangePowerState == 0) ? "" : " powering "+(isDecommission ? "off " : "on ")+toChangePowerState+" task tracker"+
                                                         (toChangePowerState > 1 ? "s" : "");
      String decommissionMsg = (toDeRecommission == 0) ? "" : " "+(isDecommission ? "de" : "re")+"commissioning "+toDeRecommission+" task tracker"+
                                                         (toDeRecommission > 1 ? "s" : "") + (toChangePowerState > 0 ? ";" : "");
      return decommissionMsg + powerOffMsg;
   }

   private Set<String> getActiveVmIds(Set<String> activeDnsNames) {
      ClusterMap clusterMap = null;
      Set<String> result = null;
      try {
         clusterMap = getAndReadLockClusterMap();
         Map<String, String> dnsNameMap = clusterMap.getVmIdsForDnsNames(activeDnsNames);
         if (dnsNameMap != null) {
            result = new HashSet<String>(dnsNameMap.values());
         }
      } finally {
         unlockClusterMap(clusterMap);
      }
      return result;
   }

   private Set<String> getValidDnsNames(Map<String, String> hostNames) {
      Set<String> result = null;
      for (String dnsName : hostNames.values()) {
         if ((dnsName != null) && (dnsName.trim().length() > 0)) {
            if (result == null) {
               result = new HashSet<String>();
            }
            result.add(dnsName);
         }
      }
      return result;
   }

   private Set<String> getSuccessfullyDisabledVmIds(Set<String> allVmIds, Set<String> activeVmIds) {
      Set<String> result = new HashSet<String>(allVmIds);
      if (activeVmIds != null) {
         result.removeAll(activeVmIds);
      }
      return result;
   }

   @Override
   public Set<String> getActiveTTs(String clusterId) throws Exception {
      HadoopClusterInfo hadoopCluster = null;

      ClusterMap clusterMap = null;
      try {
         clusterMap = getAndReadLockClusterMap();
         hadoopCluster = clusterMap.getHadoopInfoForCluster(clusterId);
      } finally {
         unlockClusterMap(clusterMap);
      }

      if ((hadoopCluster != null) && (hadoopCluster.getJobTrackerIpAddr() != null)) {
         return _hadoopActions.getActiveTTs(hadoopCluster, 0);
      }
      return null;
   }

}
