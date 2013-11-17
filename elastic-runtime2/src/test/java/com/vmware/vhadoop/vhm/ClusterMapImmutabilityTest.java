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

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;

/* Since the results from ClusterMap can be cached, it is critical that they are immutable */
public class ClusterMapImmutabilityTest extends AbstractJUnitTest {
   AbstractClusterMap _clusterMap;
   ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(new StandaloneSimpleVCActions(), null);

   @Override
   void processNewEventData(VMEventData eventData, String expectedClusterName, Set<ClusterScaleEvent> impliedScaleEvents) {
      String clusterName = _clusterMap.handleClusterEvent(cscl.translateVMEventData(eventData), impliedScaleEvents);
      assertEquals(expectedClusterName, clusterName);
   }

   @Override
   void registerScaleStrategy(ScaleStrategy scaleStrategy) {
      _clusterMap.registerScaleStrategy(scaleStrategy);
   }

   class ImpliedScaleEvent extends AbstractClusterScaleEvent {
      Integer _data;

      ImpliedScaleEvent(String clusterId, Integer data) {
         super("implied scale event (test)");
         setClusterId(clusterId);
         _data = data;
      }

      @Override
      public boolean isExclusive() {
         return false;
      }
   }

   @Before
   public void initialize() {
      _clusterMap = new ClusterMapImpl(new ExtraInfoToClusterMapper() {
         @Override
         public String getStrategyKey(SerengetiClusterVariableData scvd, String clusterId) {
            if ((scvd != null) && (scvd._enableAutomation)) {
               return OTHER_SCALE_STRATEGY_KEY;
            }
            return DEFAULT_SCALE_STRATEGY_KEY;
         }

         @Override
         public Map<String, String> parseExtraInfo(SerengetiClusterVariableData scvd, String clusterId) {
            return null;
         }

         @Override
         public Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData scvd, String clusterId, boolean isNewCluster, boolean isClusterViable) {
            return null;
         }
      });
   }
   
   @Test(expected=UnsupportedOperationException.class)
   public void getAllKnownClusterIds() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      _clusterMap.getAllKnownClusterIds().clear();
   }
   
   @Test(expected=UnsupportedOperationException.class)
   public void getAllClusterIdsForScaleStrategyKey() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      _clusterMap.getAllClusterIdsForScaleStrategyKey(OTHER_SCALE_STRATEGY_KEY).clear();
   }
   
   @Test(expected=UnsupportedOperationException.class)
   public void getDnsNamesForVMs() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      _clusterMap.getDnsNamesForVMs(getVmIdsFromVmNames(_vmNames)).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void getHostIdsForVMs() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      _clusterMap.getHostIdsForVMs(getVmIdsFromVmNames(_vmNames)).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void getNicAndIpAddressesForVm() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      String vmId = getVmIdsFromVmNames(_vmNames).iterator().next();
      _clusterMap.getNicAndIpAddressesForVm(vmId).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void getVmIdsForDnsNames() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      Map<String, String> dnsNames = _clusterMap.getDnsNamesForVMs(getVmIdsFromVmNames(_vmNames));
      _clusterMap.getVmIdsForDnsNames(new HashSet<String>(dnsNames.values())).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void listHostsWithComputeVMsForCluster() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      String clusterId = _clusterMap.getAllKnownClusterIds().iterator().next();
      _clusterMap.listHostsWithComputeVMsForCluster(clusterId).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void listComputeVMsForCluster() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      String clusterId = _clusterMap.getAllKnownClusterIds().iterator().next();
      _clusterMap.listComputeVMsForCluster(clusterId).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void listComputeVMsForPowerState() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      _clusterMap.listComputeVMsForPowerState(false).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void listComputeVMsForClusterAndPowerState() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      String clusterId = _clusterMap.getAllKnownClusterIds().iterator().next();
      _clusterMap.listComputeVMsForClusterAndPowerState(clusterId, false).clear();
   }

   @Test(expected=UnsupportedOperationException.class)
   public void listComputeVMsForClusterHostAndPowerState() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      String clusterId = _clusterMap.getAllKnownClusterIds().iterator().next();
      Set<String> vmIds = _clusterMap.listComputeVMsForCluster(clusterId);
      String hostId = _clusterMap.getHostIdForVm(vmIds.iterator().next());
      _clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, hostId, false).clear();
   }
}