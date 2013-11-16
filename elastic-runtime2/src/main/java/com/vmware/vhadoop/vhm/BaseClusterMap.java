package com.vmware.vhadoop.vhm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VmType;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;

/* BaseClusterMap has code for all of the methods exposed in ClusterMapImpl and CachingClusterMapImpl
 * 
 * Each method corresponds to a public method in ClusterMap with "Base" added as a post-fix to facilitate lookup using reflection
 * 
 * 
 */
public abstract class BaseClusterMap extends AbstractClusterMap {

   static final String BASE_METHOD_POSTIFX = "Base";
   
   BaseClusterMap(ExtraInfoToClusterMapper mapper) {
      super(mapper);
   }

   /* Return null if a cluster is not viable as there's no scaling we can do with it */
   String getScaleStrategyKeyBase(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      ClusterInfo ci = getCluster(clusterId);
      if ((ci != null) && isClusterViable(clusterId)) {
         return ci.getScaleStrategyKey();
      }
      return null;
   }

   Set<String> listComputeVMsForClusterAndPowerStateBase(String clusterId, boolean powerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (clusterId != null) {
         return generateComputeVMList(clusterId, null, powerState);
      }
      return null;
   }

   Set<String> listComputeVMsForPowerStateBase(boolean powerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      return generateComputeVMList(null, null, powerState);
   }

   Set<String> listComputeVMsForClusterBase(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      return generateComputeVMList(clusterId, null, null);
   }

   Set<String> listHostsWithComputeVMsForClusterBase(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         Set<String> result = new HashSet<String>();
         for (VMInfo vminfo : getVMInfoMap().values()) {
            if ((vminfo.getVmType().equals(VmType.COMPUTE)) && vminfo.getClusterId().equals(clusterId)) {
               String hostMoRef = vminfo.getHostMoRef();
               if (assertHasData(hostMoRef)) {
                  result.add(hostMoRef);
               }
            }
         }
         return (result.size() == 0) ? null : result;
      }
      return null;
   }

   Set<String> listComputeVMsForClusterHostAndPowerStateBase(String clusterId, String hostId, boolean powerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if ((clusterId != null) && (hostId != null)) {
         return generateComputeVMList(clusterId, hostId, powerState);
      }
      return null;
   }

   String getClusterIdForFolderBase(String clusterFolderName) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (clusterInfoMapHasData()) {
         for (ClusterInfo ci : getClusterInfoMap().values()) {
            String constantFolder = ci.getSerengetiFolder();     /* Set when cluster is created by Serengeti */
            String discoveredFolder = ci.getDiscoveredFolderName();            /* Discovered from SerengetiLimitInstruction */
            if (((constantFolder != null) && (constantFolder.equals(clusterFolderName))) ||
                  ((discoveredFolder != null) && (discoveredFolder.equals(clusterFolderName)))) {
               return ci.getClusterId();
            }
         }
      }
      return null;
   }

   Boolean checkPowerStateOfVmsBase(Set<String> vmIds, boolean expectedPowerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(vmIds)) {
         for (String vmId : vmIds) {
            Boolean result = checkPowerStateOfVmBase(vmId, expectedPowerState);
            if ((result == null) || (result == false)) {
               return result;
            }
         }
         return true;
      }
      return null;
   }

   Boolean checkPowerStateOfVmBase(String vmId, boolean expectedPowerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vm = getVMInfoMap().get(vmId);
         if (vm != null) {
            if (vm.getPowerState() == null) {
               return null;
            }
            return vm.getPowerState() == expectedPowerState;
         } else {
            return null;
         }
      }
      return null;
   }

   Map<String, String> getHostIdsForVMsBase(Set<String> vmIds) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(vmIds) && vmInfoMapHasData()) {
         Map<String, VMInfo> vmInfoMap = getVMInfoMap();
         Map<String, String> results = new HashMap<String, String>();
         for (String vmId : vmIds) {
            VMInfo vminfo = vmInfoMap.get(vmId);
            if ((vminfo != null) && assertHasData(vminfo.getHostMoRef())) {
               results.put(vmId, vminfo.getHostMoRef());
            }
         }
         if (results.size() > 0) {
            return results;
         }
      }
      return null;
   }

   String getDnsNameForVMBase(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vminfo = getVMInfoMap().get(vmId);
         if (vminfo != null) {
            String dnsName = vminfo.getDnsName();
            if (assertHasData(dnsName)) {
               return dnsName;
            }
         }
      }
      return null;
   }

   /* HadoopClusterInfo returned may contain null values for any of its fields except for clusterId
    * This method will return null if a JobTracker representing the cluster is powered off */
   HadoopClusterInfo getHadoopInfoForClusterBase(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      ClusterInfo ci = getCluster(clusterId);
      HadoopClusterInfo result = null;
      if (ci != null) {
         if (vmInfoMapHasData()) {
            VMInfo vi = getVMInfoMap().get(ci.getMasterMoRef());
            Boolean powerState = checkPowerStateOfVm(vi.getMoRef(), true);
            if ((vi != null) && (powerState != null) && powerState) {
               /* Constant and Variable data references are guaranteed to be non-null. iPAddress or dnsName may be null */
               result = new HadoopClusterInfo(ci.getClusterId(), vi.getDnsName(), ci.getJobTrackerPort());
            }
         }
      }
      return result;
   }

   /* Note that the method returns all valid input VM ids, even if they have null DNS names */
   Map<String, String> getDnsNamesForVMsBase(Set<String> vmIds) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(vmIds) && vmInfoMapHasData()) {
         Map<String, VMInfo> vmInfoMap = getVMInfoMap();
         Map<String, String> results = new HashMap<String, String>();
         for (String vmId : vmIds) {
            if (vmInfoMap.get(vmId) != null) {
               results.put(vmId, getDnsNameForVMBase(vmId));
            }
         }
         if (results.size() > 0) {
            return results;
         }
      }
      return null;
   }

   Map<String, String> getVmIdsForDnsNamesBase(Set<String> dnsNames) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(dnsNames) && vmInfoMapHasData()) {
         Map<String, String> results = new HashMap<String, String>();
         for (VMInfo vminfo : getVMInfoMap().values()) {
            String dnsNameToTest = vminfo.getDnsName();
            if (assertHasData(dnsNameToTest) && dnsNames.contains(dnsNameToTest)) {
               results.put(dnsNameToTest, vminfo.getMoRef());
            }
         }
         if (results.size() > 0) {
            return results;
         }
      }
      return null;
   }

   String getVmIdForDnsNameBase(String dnsName) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         for (VMInfo vminfo : getVMInfoMap().values()) {
            String dnsNameToTest = vminfo.getDnsName();
            if (assertHasData(dnsNameToTest) && (dnsName != null) && dnsNameToTest.equals(dnsName)) {
               return vminfo.getMoRef();
            }
         }
      }
      return null;
   }

   String[] getAllClusterIdsForScaleStrategyKeyBase(String key) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if ((key != null) && clusterInfoMapHasData())  {
         Set<String> result = new HashSet<String>();
         for (String clusterId : getClusterInfoMap().keySet()) {
            ScaleStrategy scaleStrategy = getScaleStrategyForCluster(clusterId);
            if ((scaleStrategy != null) && (scaleStrategy.getKey().equals(key))) {
               result.add(clusterId);
            }
         }
         if (result.size() > 0) {
            return result.toArray(new String[]{});
         }
      }
      return null;
   }
   
   String getMasterVmIdForClusterBase(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         return ci.getMasterMoRef();
      }
      return null;
   }

}
