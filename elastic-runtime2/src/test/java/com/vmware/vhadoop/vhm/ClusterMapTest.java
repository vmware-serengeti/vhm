package com.vmware.vhadoop.vhm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.events.AbstractClusterScaleEvent;
import com.vmware.vhadoop.vhm.events.ClusterScaleDecision;
import com.vmware.vhadoop.vhm.events.ClusterUpdateEvent;
import com.vmware.vhadoop.vhm.events.VmRemovedFromClusterEvent;

public class ClusterMapTest extends AbstractJUnitTest {
   ClusterMapImpl _clusterMap;
   static final String EXTRA_INFO_KEY = "extraInfo1";
   List<Boolean> _isNewClusterResult;
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
         public Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData scvd, String clusterId, boolean isNewCluster) {
            _isNewClusterResult.add(isNewCluster);
            if (!isNewCluster && (scvd != null) && (scvd._minInstances != null)) {
               int minInstances = scvd._minInstances;
               if (minInstances >= 0) {
                  Set<ClusterScaleEvent> newSet = new HashSet<ClusterScaleEvent>();
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
      String[] toTest = _clusterMap.getAllKnownClusterIds();
      assertEquals(numClusterIds, toTest.length);
      List<String> testNames = Arrays.asList(toTest);
      for (String masterVmName : _masterVmNames) {
         assertTrue(testNames.contains(getClusterIdForMasterVmName(masterVmName)));
      }
   }

   @Test
   public void getClusterIdForFolder() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, false);
      assertEquals(numClusterIds, _clusterNames.size());
      int cntr = 0;
      for (String clusterName : _clusterNames) {
         String folderName = getFolderNameForClusterName(clusterName);
         String clusterId = _clusterMap.getClusterIdForFolder(folderName);
         assertEquals(deriveClusterIdFromClusterName(clusterName), clusterId);
         
         String newFolderName = "NEW_FOLDER"+cntr++;
         _clusterMap.associateFolderWithCluster(clusterId, newFolderName);
         assertEquals(clusterId, _clusterMap.getClusterIdForFolder(newFolderName));
      }

      /* Negative tests */
      assertNull(_clusterMap.getClusterIdForFolder("bogus"));
      assertNull(_clusterMap.getClusterIdForFolder(null));
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
   public void getDnsNameForVMs() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, false);
      Set<String> vmIds = getVmIdsFromVmNames(_vmNames);
      Set<String> dnsNames = _clusterMap.getDnsNameForVMs(vmIds);
      assertEquals(_vmNames.size(), dnsNames.size());
      for (String vmName : _vmNames) {
         assertTrue(dnsNames.contains(getDnsNameFromVmName(vmName)));
      }

      /* Negative tests */
      assertNull(_clusterMap.getDnsNameForVMs(getBogusSet()));
      assertNull(_clusterMap.getDnsNameForVMs(getEmptySet()));
      assertNull(_clusterMap.getDnsNameForVMs(null));
   }

   @Test
   public void getHadoopInfoForCluster() {
      int numClusterIds = 3;
      populateSimpleClusterMap(numClusterIds, 4, false);
      for (String clusterName : _clusterNames) {
         String clusterId = deriveClusterIdFromClusterName(clusterName);
         HadoopClusterInfo hci = _clusterMap.getHadoopInfoForCluster(clusterId);
         assertNotNull(hci);
         assertEquals(deriveMasterIpAddrFromClusterId(clusterId), hci.getJobTrackerAddr());
         assertEquals((Integer)DEFAULT_PORT, hci.getJobTrackerPort());
      }

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
      
      assertEquals(1, _clusterMap.getAllClusterIdsForScaleStrategyKey(OTHER_SCALE_STRATEGY_KEY).length);
      assertEquals(2, _clusterMap.getAllClusterIdsForScaleStrategyKey(DEFAULT_SCALE_STRATEGY_KEY).length);
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
   
   private ClusterUpdateEvent createNewMinInstancesUpdate(Integer newInstances, String clusterName) {
      String masterVmName = getMasterVmNameForCluster(clusterName);
      SerengetiClusterVariableData cvd = new SerengetiClusterVariableData();
      cvd._minInstances = newInstances;
      ClusterUpdateEvent cue = new ClusterUpdateEvent(getVmIdFromVmName(masterVmName), cvd);
      return cue;
   }
   
   @Test
   public void testImpliedScaleEvents() {
      String clusterName = CLUSTER_NAME_PREFIX+0;
      
      /* Create new cluster */
      populateClusterSameHost(clusterName, "DEFAULT_HOST1", 4, false, false, 0, null);
      assertEquals(1, _isNewClusterResult.size());
      assertEquals(true, _isNewClusterResult.get(0));
      
      Integer newInstances = 3;
      /* This set will contain any implied events created by the cluster state change */
      Set<ClusterScaleEvent> impliedScaleEventsResultSet = new HashSet<ClusterScaleEvent>();

      _clusterMap.handleClusterEvent(createNewMinInstancesUpdate(newInstances, clusterName), impliedScaleEventsResultSet);
      assertEquals(1, impliedScaleEventsResultSet.size());  /* We should have a new event */
      assertEquals(2, _isNewClusterResult.size());          
      assertEquals(false, _isNewClusterResult.get(1));      /* This is not a new cluster, rather an update to an existing cluster */
      
      /* Validate the contents of the generated event */
      ImpliedScaleEvent impliedScaleEvent = (ImpliedScaleEvent)impliedScaleEventsResultSet.iterator().next();
      assertEquals(newInstances, impliedScaleEvent._data);
      impliedScaleEventsResultSet.clear();
      
      /* Create a new cluster map update, but one which our ExtraInfoToClusterMapper has been coded to ignore */
      _clusterMap.handleClusterEvent(createNewMinInstancesUpdate(-1, clusterName), impliedScaleEventsResultSet);
      assertEquals(0, impliedScaleEventsResultSet.size());    /* Verify that an event was not generated */
      assertEquals(3, _isNewClusterResult.size());             /* Verify that getImpliedScaleEventsForUpdate was invoked */ 
      assertEquals(false, _isNewClusterResult.get(2));
   }
   
   @Test
   public void invokeGettersOnEmptyClusterMap() {
      Set<String> vms = new HashSet<String>();
      vms.add("foo");
      assertNull(_clusterMap.getAllKnownClusterIds());
      assertNull(_clusterMap.getClusterIdForFolder("foo"));
      assertNull(_clusterMap.getClusterIdForVm("foo"));
      assertNull(_clusterMap.getDnsNameForVMs(vms));
      assertNull(_clusterMap.getHadoopInfoForCluster("foo"));
      assertNull(_clusterMap.getHostIdForVm("foo"));
      assertNull(_clusterMap.getHostIdsForVMs(vms));
      assertNull(_clusterMap.getLastClusterScaleCompletionEvent("foo"));
      assertNull(_clusterMap.getScaleStrategyForCluster("foo"));
      assertNull(_clusterMap.listComputeVMsForClusterAndPowerState("foo", false));
      assertNull(_clusterMap.listComputeVMsForClusterHostAndPowerState("foo", "foo", false));
      assertNull(_clusterMap.listComputeVMsForPowerState(false));
      assertNull(_clusterMap.listHostsWithComputeVMsForCluster("foo"));
      assertNull(_clusterMap.checkPowerStateOfVms(vms, false));
      assertNull(_clusterMap.getNumVCPUsForVm("foo"));
      assertNull(_clusterMap.getPowerOnTimeForVm("foo"));
      assertNull(_clusterMap.getExtraInfo("foo", "bar"));
      assertNull(_clusterMap.getAllClusterIdsForScaleStrategyKey("foo"));
   }
}
