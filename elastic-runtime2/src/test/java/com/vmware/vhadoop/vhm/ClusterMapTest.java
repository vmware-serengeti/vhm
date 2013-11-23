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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMVariableData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;
import com.vmware.vhadoop.vhm.events.ClusterUpdateEvent;
import com.vmware.vhadoop.vhm.events.VmRemovedFromClusterEvent;
import com.vmware.vhadoop.vhm.events.VmUpdateEvent;

public class ClusterMapTest extends AbstractJUnitTest {
   AbstractClusterMap _clusterMap;
   static final String EXTRA_INFO_KEY = "extraInfo1";
   List<Boolean> _isNewClusterResult;
   List<Boolean> _isClusterViableResult;
   ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(new StandaloneSimpleVCActions(), null);

   @Override
   void processNewEventData(VMEventData eventData, String expectedClusterId, Set<ClusterScaleEvent> impliedScaleEvents) {
      String clusterId = _clusterMap.handleClusterEvent(cscl.translateVMEventData(eventData), impliedScaleEvents);
      assertEquals(expectedClusterId, clusterId);
   }

   private void changeMasterVmPowerStateForClusterName(String clusterName, boolean powerState) {
      VMVariableData vmVariableData = new VMVariableData();
      vmVariableData._powerState = powerState;
      _clusterMap.handleClusterEvent(new VmUpdateEvent(getMasterVmIdForCluster(clusterName), vmVariableData), null);
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
      _isNewClusterResult = new ArrayList<Boolean>();
      _isClusterViableResult = new ArrayList<Boolean>();
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
            Map<String, String> result = null;
            if (scvd != null) {
               Integer minInstances = scvd._minInstances;
               if (minInstances != null) {
                  result = new HashMap<String, String>();
                  result.put(EXTRA_INFO_KEY, minInstances.toString());
               }
            }
            return result;
         }

         @Override
         /* All new clusters will be non-viable, but not all non-viable clusters will be new */
         public Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData scvd, String clusterId, boolean isNewCluster, boolean isClusterViable) {
            _isNewClusterResult.add(isNewCluster);
            _isClusterViableResult.add(isClusterViable);
            if (!isNewCluster && (scvd != null) && (scvd._minInstances != null)) {
               int minInstances = scvd._minInstances;
               if (minInstances >= 0) {
                  Set<ClusterScaleEvent> newSet = new LinkedHashSet<ClusterScaleEvent>();
                  newSet.add(new ImpliedScaleEvent(clusterId, minInstances));
                  return newSet;
               }
            }
            return null;
         }
      });
   }

   @Test
   public void getAllKnownClusterIds() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, false);
      Set<String> knownClusterIds = _clusterMap.getAllKnownClusterIds();
      assertEquals(numClusterIds, knownClusterIds.size());
      
      for (String masterVmName : _masterVmNames) {
         String clusterId = getClusterIdForMasterVmName(masterVmName);
         assertTrue(knownClusterIds.contains(clusterId));
         String masterVmId = _clusterMap.getMasterVmIdForCluster(clusterId);
         assertEquals(getVmIdFromVmName(masterVmName), masterVmId);
      }

      assertNull(_clusterMap.getMasterVmIdForCluster("bogus"));
      assertNull(_clusterMap.getMasterVmIdForCluster(null));
   }

   @Test
   public void getClusterIdForName() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, false);
      assertEquals(numClusterIds, _clusterNames.size());
      for (String clusterName : _clusterNames) {
         String clusterId = _clusterMap.getClusterIdForName(clusterName);
         assertEquals(deriveClusterIdFromClusterName(clusterName), clusterId);
      }

      /* Negative tests */
      assertNull(_clusterMap.getClusterIdForName("bogus"));
      assertNull(_clusterMap.getClusterIdForName(null));
   }

   @Test
   public void getClusterIdForVm() {
      int numClusterIds = 3;
      int numVmsPerCluster = 4;
      populateSimpleClusterMap(numClusterIds, numVmsPerCluster, false);
      assertEquals(numClusterIds * numVmsPerCluster, _vmNames.size());
      for (String vmName : _vmNames) {
         String vmId = getVmIdFromVmName(vmName);
         assertEquals(deriveClusterIdFromVmName(vmName), _clusterMap.getClusterIdForVm(vmId));
      }
      
      /* Negative tests */
      assertNull(_clusterMap.getClusterIdForVm("bogus"));
      assertNull(_clusterMap.getClusterIdForVm(null));
   }

   @Test
   public void getDnsNamesForVMs() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, true);
      Set<String> vmIds = getVmIdsFromVmNames(_vmNames);
      Map<String, String> dnsNames = _clusterMap.getDnsNamesForVMs(vmIds);
      assertEquals(_vmNames.size(), dnsNames.size());
      for (String vmName : _vmNames) {
         String vmId = getVmIdFromVmName(vmName);
         assertEquals(dnsNames.get(vmId), getDnsNameFromVmName(vmName));
      }

      /* Negative tests */
      assertNull(_clusterMap.getDnsNamesForVMs(getBogusSet()));
      assertNull(_clusterMap.getDnsNamesForVMs(getEmptySet()));
      assertNull(_clusterMap.getDnsNamesForVMs(null));

      assertNull(_clusterMap.getDnsNameForVM("bogus"));
      assertNull(_clusterMap.getDnsNameForVM(null));
   }

   @Test
   public void getVmIdsForDnsName() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, true);
      Set<String> vmIds = getVmIdsFromVmNames(_vmNames);
      Map<String, String> dnsNames = _clusterMap.getDnsNamesForVMs(vmIds);
      Map<String, String> vmIdMap = _clusterMap.getVmIdsForDnsNames(new HashSet<String>(dnsNames.values()));
      assertEquals(dnsNames.size(), vmIdMap.size());
      for (String vmId : vmIdMap.values()) {
         assertTrue(vmIds.contains(vmId));
      }

      /* Negative tests */
      assertNull(_clusterMap.getVmIdsForDnsNames(getBogusSet()));
      assertNull(_clusterMap.getVmIdsForDnsNames(getEmptySet()));
      assertNull(_clusterMap.getVmIdsForDnsNames(null));

      assertNull(_clusterMap.getVmIdForDnsName("bogus"));
      assertNull(_clusterMap.getVmIdForDnsName(null));
   }

   @Test
   public void getHadoopInfoForCluster() {
      String clusterName0 = CLUSTER_NAME_PREFIX+0;
      String clusterName1 = CLUSTER_NAME_PREFIX+1;
      populateClusterSameHost(clusterName0, "DEFAULT_HOST", 4, true, false, 0, null);
      populateClusterSameHost(clusterName1, "DEFAULT_HOST", 4, false, false, 1, null);

      String clusterId = deriveClusterIdFromClusterName(clusterName0);
      HadoopClusterInfo hci = _clusterMap.getHadoopInfoForCluster(clusterId);
      assertNotNull(hci);
      assertEquals(deriveMasterDnsNameFromClusterId(clusterId), hci.getJobTrackerDnsName());
      assertEquals((Integer)DEFAULT_PORT, hci.getJobTrackerPort());
      String masterVmName = getMasterVmNameForCluster(clusterName0);
      String masterDnsName = getDnsNameFromVmName(masterVmName);
      assertEquals(masterDnsName, hci.getJobTrackerDnsName());

      changeMasterVmPowerStateForClusterName(clusterName1, false);
      clusterId = deriveClusterIdFromClusterName(clusterName1);
      hci = _clusterMap.getHadoopInfoForCluster(clusterId);
      assertNull(hci);        /* If the cluster is not powered on, we should get null */

      /* Negative tests */
      assertNull(_clusterMap.getHadoopInfoForCluster("bogus"));
      assertNull(_clusterMap.getHadoopInfoForCluster(null));
   }

   @Test
   public void getHostIdForVM() {
      int numClusterIds = 3;
      populateClusterPerHost(numClusterIds, 4, false);
      for (String vmName : _vmNames) {
         String hostId = _clusterMap.getHostIdForVm(getVmIdFromVmName(vmName));
         String expected = deriveHostIdFromVmName(vmName);
         assertEquals(expected, hostId);
      }

      /* Negative tests */
      assertNull(_clusterMap.getHostIdForVm("bogus"));
      assertNull(_clusterMap.getHostIdForVm(null));
   }

   @Test
   public void getHostIdsForVMs() {
      int numClusterIds = 3;
      populateClusterPerHost(numClusterIds, 4, false);
      Set<String> vmIds = getVmIdsFromVmNames(_vmNames);
      Map<String, String> hostIds = _clusterMap.getHostIdsForVMs(vmIds);
      assertEquals(vmIds.size(), hostIds.keySet().size());
      for (String vmId : vmIds) {
         String hostId = hostIds.get(vmId);
         assertNotNull(hostId);
         assertEquals(deriveHostIdFromVmId(vmId), hostId);
      }

      /* Negative tests */
      assertNull(_clusterMap.getHostIdsForVMs(getBogusSet()));
      assertNull(_clusterMap.getHostIdsForVMs(getEmptySet()));
      assertNull(_clusterMap.getHostIdsForVMs(null));
   }

   @Test
   public void getLastClusterScaleCompletionEvent() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, false);
      for (String clusterName : _clusterNames) {
         String clusterId = deriveClusterIdFromClusterName(clusterName);
         assertNull(_clusterMap.getLastClusterScaleCompletionEvent(clusterId));

         ClusterScaleCompletionEvent cse = new ClusterScaleDecision(clusterId);
         _clusterMap.handleCompletionEvent(cse);
         assertEquals(cse, _clusterMap.getLastClusterScaleCompletionEvent(clusterId));

         /* Check that most recent event is returned */
         ClusterScaleCompletionEvent cse2 = new ClusterScaleDecision(clusterId);
         _clusterMap.handleCompletionEvent(cse2);
         assertEquals(cse2, _clusterMap.getLastClusterScaleCompletionEvent(clusterId));
      }

      /* Negative tests */
      assertNull(_clusterMap.getLastClusterScaleCompletionEvent(null));
      assertNull(_clusterMap.getLastClusterScaleCompletionEvent("bogus"));
   }

   @Test
   public void testScaleStrategies() {
      populateClusterSameHost(CLUSTER_NAME_PREFIX+0, "DEFAULT_HOST", 4, false, false, 0, null);
      populateClusterSameHost(CLUSTER_NAME_PREFIX+1, "DEFAULT_HOST", 4, false, false, 1, null);
      populateClusterSameHost(CLUSTER_NAME_PREFIX+2, "DEFAULT_HOST", 4, false, true, 2, null);

      String cid0 = deriveClusterIdFromClusterName(CLUSTER_NAME_PREFIX+0);
      ScaleStrategy ss0 = _clusterMap.getScaleStrategyForCluster(cid0);
      assertEquals(DEFAULT_SCALE_STRATEGY_KEY, ss0.getKey());

      String cid1 = deriveClusterIdFromClusterName(CLUSTER_NAME_PREFIX+1);
      ScaleStrategy ss1 = _clusterMap.getScaleStrategyForCluster(cid1);
      assertEquals(DEFAULT_SCALE_STRATEGY_KEY, ss1.getKey());

      String cid2 = deriveClusterIdFromClusterName(CLUSTER_NAME_PREFIX+2);
      ScaleStrategy ss2 = _clusterMap.getScaleStrategyForCluster(cid2);
      assertEquals(OTHER_SCALE_STRATEGY_KEY, ss2.getKey());

      ScaleStrategy ssBogus = _clusterMap.getScaleStrategyForCluster("bogus");
      assertNull(ssBogus);

      /* Change ss1 to use AUTO */
      ClusterUpdateEvent scaleStrategyChange = createScaleStrategyChangeEvent(CLUSTER_NAME_PREFIX+1, true);
      _clusterMap.handleClusterEvent(scaleStrategyChange, null);

      ss1 = _clusterMap.getScaleStrategyForCluster(cid1);
      assertEquals(OTHER_SCALE_STRATEGY_KEY, ss1.getKey());
      assertEquals(OTHER_SCALE_STRATEGY_KEY, _clusterMap.getScaleStrategyKey(cid1));

      /* Negative tests */
      assertNull(_clusterMap.getScaleStrategyForCluster(null));
      assertNull(_clusterMap.getScaleStrategyForCluster("bogus"));

      assertNull(_clusterMap.getScaleStrategyKey(null));
      assertNull(_clusterMap.getScaleStrategyKey("bogus"));
   }

   private ClusterUpdateEvent createScaleStrategyChangeEvent(String clusterName, boolean enableAuto) {
      String masterVmId = getMasterVmIdForCluster(clusterName);
      SerengetiClusterVariableData vData = new SerengetiClusterVariableData();
      vData._enableAutomation = enableAuto;
      ClusterUpdateEvent cue = new ClusterUpdateEvent(masterVmId, vData);
      return cue;
   }

   @Test
   public void powerStateAndListTests() {
      /* Create 3 clusters, each with 3 compute VMs and a master. Two have vms powered one and one cluster is powered off */
      populateClusterSameHost(CLUSTER_NAME_PREFIX+0, "DEFAULT_HOST1", 4, false, false, 0, null);
      populateClusterSameHost(CLUSTER_NAME_PREFIX+1, "DEFAULT_HOST1", 4, true, false, 1, null);
      populateClusterSameHost(CLUSTER_NAME_PREFIX+2, "DEFAULT_HOST2", 4, true, true, 2, null);

      /* Note expected result is 3, not 4, since 3 VMs are compute VMs and 1 is master */
      Integer[][] expectedSizes1 = new Integer[][]{new Integer[]{3, null}, new Integer[]{null, 3}, new Integer[]{null, 3}};
      boolean[] expectMatch1 = new boolean[]{true, true, false};
      boolean[] expectMatch2 = new boolean[]{false, false, true};

      /* For each cluster */
      for (int i=0; i<3; i++) {
         String clusterName = CLUSTER_NAME_PREFIX+i;
         String clusterId = deriveClusterIdFromClusterName(clusterName);

         /* For each power state */
         for (int j=0; j<2; j++) {
            boolean expectedPowerState = (j==1);
            Set<String> computeVMs = _clusterMap.listComputeVMsForClusterAndPowerState(clusterId, expectedPowerState);
            Integer result = (computeVMs == null) ? null : computeVMs.size();
            assertEquals(expectedSizes1[i][j], result);

            if (computeVMs != null) {
               assertTrue(_clusterMap.checkPowerStateOfVms(computeVMs, expectedPowerState));
               assertFalse(_clusterMap.checkPowerStateOfVms(computeVMs, !expectedPowerState));

               if (expectedPowerState) {
                  assertNotNull(_clusterMap.getPowerOnTimeForVm(computeVMs.iterator().next()));
               }

               assertEquals(clusterId, _clusterMap.getClusterIdFromVMs(new ArrayList<String>(computeVMs)));

               /* Check that the compute VM names returned contain the cluster name */
               for (String computeVM : computeVMs) {
                  assertTrue(computeVM.contains(clusterName));
               }

               /* Add host restrictions and check results against original results */
               Set<String> computeVMs2 = _clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, MOREF_PREFIX+"DEFAULT_HOST1", (j==1));
               if (expectMatch1[i]) {
                  assertEquals(computeVMs.size(), computeVMs2.size());
               } else if (computeVMs2 != null) {
                  assertTrue(computeVMs.size() != computeVMs2.size());
               }

               computeVMs2 = _clusterMap.listComputeVMsForClusterHostAndPowerState(clusterId, MOREF_PREFIX+"DEFAULT_HOST2", (j==1));
               if (expectMatch2[i]) {
                  assertEquals(computeVMs.size(), computeVMs2.size());
               } else if (computeVMs2 != null) {
                  assertTrue(computeVMs.size() != computeVMs2.size());
               }
            }
         }

         Set<String> hostIds = _clusterMap.listHostsWithComputeVMsForCluster(clusterId);
         assertEquals(1, hostIds.size());

         String extraInfo = _clusterMap.getExtraInfo(clusterId, EXTRA_INFO_KEY);
         assertEquals(Integer.parseInt(extraInfo), i);
      }

      assertEquals(3, _clusterMap.listComputeVMsForPowerState(false).size());
      assertEquals(6, _clusterMap.listComputeVMsForPowerState(true).size());

      /* Negative tests */
      assertNull(_clusterMap.listComputeVMsForClusterAndPowerState("bogus", false));
      assertNull(_clusterMap.listComputeVMsForClusterAndPowerState(null, false));

      assertNull(_clusterMap.listComputeVMsForClusterHostAndPowerState("bogus", "bogus", false));
      assertNull(_clusterMap.listComputeVMsForClusterHostAndPowerState(null, null, false));

      assertNull(_clusterMap.checkPowerStateOfVms(null, false));
      assertNull(_clusterMap.checkPowerStateOfVms(getEmptySet(), false));
      assertNull(_clusterMap.checkPowerStateOfVms(getBogusSet(), false));

      assertNull(_clusterMap.checkPowerStateOfVm(null, false));
      assertNull(_clusterMap.checkPowerStateOfVm("bogus", false));

      assertNull(_clusterMap.listHostsWithComputeVMsForCluster("bogus"));
      assertNull(_clusterMap.listHostsWithComputeVMsForCluster(null));

      assertNull(_clusterMap.getClusterIdFromVMs(null));
      assertNull(_clusterMap.getClusterIdFromVMs(new ArrayList<String>(getEmptySet())));
      assertNull(_clusterMap.getClusterIdFromVMs(new ArrayList<String>(getBogusSet())));

      assertNull(_clusterMap.getPowerOnTimeForVm("bogus"));
      assertNull(_clusterMap.getPowerOnTimeForVm(null));

      assertNull(_clusterMap.getExtraInfo("bogus", null));
      assertNull(_clusterMap.getExtraInfo(null, null));
      assertNull(_clusterMap.getExtraInfo(null, "bogus"));
      assertNull(_clusterMap.getExtraInfo("bogus", "bogus"));

      assertEquals(1, _clusterMap.getAllClusterIdsForScaleStrategyKey(OTHER_SCALE_STRATEGY_KEY).size());
      assertEquals(2, _clusterMap.getAllClusterIdsForScaleStrategyKey(DEFAULT_SCALE_STRATEGY_KEY).size());
      assertNull(_clusterMap.getAllClusterIdsForScaleStrategyKey(null));
      assertNull(_clusterMap.getAllClusterIdsForScaleStrategyKey("bogus"));
   }

   @Test
   public void getVCPU() {
      populateClusterSameHost(CLUSTER_NAME_PREFIX+0, "DEFAULT_HOST1", 4, false, false, 0, null);
      for (String vmName : _vmNames) {
         String vmId = getVmIdFromVmName(vmName);
         assertEquals((Integer)DEFAULT_VCPUS, _clusterMap.getNumVCPUsForVm(vmId));
      }

      /* Negative tests */
      assertNull(_clusterMap.getNumVCPUsForVm("bogus"));
      assertNull(_clusterMap.getNumVCPUsForVm(null));
   }

   @Test
   public void testRemoveVM() {
      String clusterName = CLUSTER_NAME_PREFIX+0;
      String clusterId = deriveClusterIdFromClusterName(clusterName);
      populateClusterSameHost(clusterName, "DEFAULT_HOST1", 4, false, false, 0, null);
      Set<String> vms = _clusterMap.listComputeVMsForClusterAndPowerState(clusterId, false);
      int runningTotal = 3;
      assertEquals(runningTotal, vms.size());

      /* Remove the compute VMs */
      for (String vmId : vms) {
         --runningTotal;
         _clusterMap.handleClusterEvent(new VmRemovedFromClusterEvent(vmId), null);
         Set<String> remaining = _clusterMap.listComputeVMsForClusterAndPowerState(clusterId, false);
         if (runningTotal > 0) {
            assertEquals(runningTotal, remaining.size());
            assertFalse(remaining.contains(vmId));
         } else {
            assertNull(remaining);
         }
      }

      /* Cluster should still be there */
      String masterVmId = getMasterVmIdForCluster(clusterName);
      assertEquals(clusterId, _clusterMap.getClusterIdForVm(masterVmId));

      /* Remove master VM and the cluster should be removed also */
      _clusterMap.handleClusterEvent(new VmRemovedFromClusterEvent(masterVmId), null);
      assertNull(_clusterMap.getClusterIdForVm(getVmIdFromVmName(masterVmId)));
      assertNull(_clusterMap.getAllKnownClusterIds());
   }

   private ClusterUpdateEvent createNewInstancesChangeUpdate(Integer newMinInstances, Integer newMaxInstances, String clusterName) {
      String masterVmName = getMasterVmNameForCluster(clusterName);
      SerengetiClusterVariableData cvd = new SerengetiClusterVariableData();
      cvd._minInstances = newMinInstances;
      cvd._maxInstances = newMaxInstances;
      ClusterUpdateEvent cue = new ClusterUpdateEvent(getVmIdFromVmName(masterVmName), cvd);
      return cue;
   }
   
   @Test
   public void testImpliedScaleEvents() {
      String clusterName = CLUSTER_NAME_PREFIX+0;

      /* Create new non-viable cluster */
      populateClusterSameHost("NonViableCluster", "DEFAULT_HOST1", 4, false, false, 0, null);
      assertEquals(1, _isNewClusterResult.size());
      assertEquals(false, _isClusterViableResult.get(0));
      assertEquals(true, _isNewClusterResult.get(0));

      populateClusterSameHost(clusterName, "DEFAULT_HOST1", 4, true, false, 0, null);
      assertEquals(2, _isNewClusterResult.size());
      assertEquals(false, _isClusterViableResult.get(1));
      assertEquals(true, _isNewClusterResult.get(1));

      Integer newInstances = 3;
      /* This set will contain any implied events created by the cluster state change */
      Set<ClusterScaleEvent> impliedScaleEventsResultSet = new HashSet<ClusterScaleEvent>();

      _clusterMap.handleClusterEvent(createNewInstancesChangeUpdate(newInstances, -1, clusterName), impliedScaleEventsResultSet);
      assertEquals(1, impliedScaleEventsResultSet.size());  /* We should have a new event */
      assertEquals(3, _isNewClusterResult.size());
      assertEquals(true, _isClusterViableResult.get(2));
      assertEquals(false, _isNewClusterResult.get(2));      /* This is not a new cluster, rather an update to an existing cluster */

      /* Validate the contents of the generated event */
      ImpliedScaleEvent impliedScaleEvent = (ImpliedScaleEvent)impliedScaleEventsResultSet.iterator().next();
      assertEquals(newInstances, impliedScaleEvent._data);
      impliedScaleEventsResultSet.clear();

      /* Create a new cluster map update, but one which our ExtraInfoToClusterMapper has been coded to ignore */
      _clusterMap.handleClusterEvent(createNewInstancesChangeUpdate(-1, -1, clusterName), impliedScaleEventsResultSet);
      assertEquals(0, impliedScaleEventsResultSet.size());    /* Verify that an event was not generated */
      assertEquals(4, _isNewClusterResult.size());             /* Verify that getImpliedScaleEventsForUpdate was invoked */
      assertEquals(true, _isClusterViableResult.get(3));
      assertEquals(false, _isNewClusterResult.get(3));
   }

   @Test
   public void validateClusterCompleteness() {
      String hostName = "DEFAULT_HOST1";
      populateClusterSameHost(CLUSTER_NAME_PREFIX+0, hostName, 4, false, false, 0, null);      /* Complete cluster, powered off */
      populateClusterSameHost(CLUSTER_NAME_PREFIX+1, hostName, 4, true, false, 0, null);       /* Complete cluster, powered on */
      populateClusterSameHost(CLUSTER_NAME_PREFIX+2, hostName, 1, false, false, 0, null);   /* Incomplete cluster - no compute VMs */

      String clusterId0 = deriveClusterIdFromClusterName(CLUSTER_NAME_PREFIX+0);
      assertTrue(_clusterMap.validateClusterCompleteness(clusterId0, 0));
      assertTrue(_clusterMap.validateClusterCompleteness(clusterId0, 500));

      String clusterId1 = deriveClusterIdFromClusterName(CLUSTER_NAME_PREFIX+1);
      assertTrue(_clusterMap.validateClusterCompleteness(clusterId1, 0));
      assertTrue(_clusterMap.validateClusterCompleteness(clusterId1, 500));

      String clusterId2 = deriveClusterIdFromClusterName(CLUSTER_NAME_PREFIX+2);
      assertFalse(_clusterMap.validateClusterCompleteness(clusterId2, 0));
      assertFalse(_clusterMap.validateClusterCompleteness(clusterId2, 500));

      try {
         Thread.sleep(1000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }

      assertFalse(_clusterMap.validateClusterCompleteness(clusterId2, 0));
      /* This should return null as the window has elapsed */
      assertNull(_clusterMap.validateClusterCompleteness(clusterId2, 500));

      /* Add a compute VM to cluster2 to make it complete */
      String clusterName2 = CLUSTER_NAME_PREFIX+2;
      VMEventData eventData = createEventData(clusterName2, clusterName2+"_"+VM_NAME_PREFIX+1, false, true, hostName,
            getMasterVmNameForCluster(clusterName2), false, -1, -1, true);
      processNewEventData(eventData, clusterId2, null);

      /* Assert that it is now complete */
      assertTrue(_clusterMap.validateClusterCompleteness(clusterId2, 0));
      assertTrue(_clusterMap.validateClusterCompleteness(clusterId2, 500));

      assertNull(_clusterMap.validateClusterCompleteness("bogus", 0));
      assertNull(_clusterMap.validateClusterCompleteness("bogus", 1000));
   }

   @Test
   public void invokeGettersOnEmptyClusterMap() {
      Set<String> emptySet = new HashSet<String>();
      emptySet.add("foo");
      assertNull(_clusterMap.getAllKnownClusterIds());
      assertNull(_clusterMap.getClusterIdForName("foo"));
      assertNull(_clusterMap.getClusterIdForVm("foo"));
      assertNull(_clusterMap.getDnsNamesForVMs(emptySet));
      assertNull(_clusterMap.getDnsNameForVM("foo"));
      assertNull(_clusterMap.getVmIdsForDnsNames(emptySet));
      assertNull(_clusterMap.getVmIdForDnsName("foo"));
      assertNull(_clusterMap.getHadoopInfoForCluster("foo"));
      assertNull(_clusterMap.getHostIdForVm("foo"));
      assertNull(_clusterMap.getHostIdsForVMs(emptySet));
      assertNull(_clusterMap.getLastClusterScaleCompletionEvent("foo"));
      assertNull(_clusterMap.getScaleStrategyForCluster("foo"));
      assertNull(_clusterMap.listComputeVMsForClusterAndPowerState("foo", false));
      assertNull(_clusterMap.listComputeVMsForClusterHostAndPowerState("foo", "foo", false));
      assertNull(_clusterMap.listComputeVMsForPowerState(false));
      assertNull(_clusterMap.listHostsWithComputeVMsForCluster("foo"));
      assertNull(_clusterMap.checkPowerStateOfVms(emptySet, false));
      assertNull(_clusterMap.checkPowerStateOfVm("foo", false));
      assertNull(_clusterMap.getNumVCPUsForVm("foo"));
      assertNull(_clusterMap.getPowerOnTimeForVm("foo"));
      assertNull(_clusterMap.getExtraInfo("foo", "bar"));
      assertNull(_clusterMap.getAllClusterIdsForScaleStrategyKey("foo"));
      assertNull(_clusterMap.validateClusterCompleteness("foo", 0));
      assertNull(_clusterMap.getMasterVmIdForCluster("foo"));
   }
}
