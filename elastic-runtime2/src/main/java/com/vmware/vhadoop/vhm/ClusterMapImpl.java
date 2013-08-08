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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VmType;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vhadoop.vhm.events.ClusterUpdateEvent;
import com.vmware.vhadoop.vhm.events.MasterVmUpdateEvent;
import com.vmware.vhadoop.vhm.events.NewMasterVMEvent;
import com.vmware.vhadoop.vhm.events.NewVmEvent;
import com.vmware.vhadoop.vhm.events.VmRemovedFromClusterEvent;
import com.vmware.vhadoop.vhm.events.VmUpdateEvent;

/* Note that this class allows multiple readers and a single writer
 * All of the methods in ClusterMap can be accessed by multiple threads, but should only ever read and are idempotent
 * The writer of ClusterMap will block until the readers have finished reading and will block new readers until it has finished updating
 * VHM controls the multi-threaded access to ClusterMap through ClusterMapAccess.
 * There should be no need for synchronization in this class provided this model is adhered to */
public class ClusterMapImpl implements ClusterMap {
   private static final Logger _log = Logger.getLogger(ClusterMap.class.getName());

   private final Map<String, ClusterInfo> _clusters = new HashMap<String, ClusterInfo>();
   private final Map<String, VMInfo> _vms = new HashMap<String, VMInfo>();
   private final Map<String, ScaleStrategy> _scaleStrategies = new HashMap<String, ScaleStrategy>();

   private final ExtraInfoToClusterMapper _extraInfoMapper;
//   private final Random _random = new Random();     /* Uncomment to do random failure testing */
//   private final int FAILURE_FACTOR = 20;
   
   ClusterMapImpl(ExtraInfoToClusterMapper mapper) {
      _extraInfoMapper = mapper;
   }

   private class VMInfo {
      final String _moRef;
      final VMConstantData _constantData;
      final VMVariableData _variableData;
      final String _clusterId;

      /* VMInfo is created with a completed VMConstantData and a potentially incomplete or even null variableData
       * moRef and clusterId must not be null */
      VMInfo(String moRef, VMConstantData constantData,
            VMVariableData variableData, String clusterId) {
         _moRef = moRef;
         _constantData = constantData;
         /* Provide an empty variableData if a null one is passed in */
         _variableData = (variableData != null) ? variableData : new VMVariableData();
         _clusterId = clusterId;
         if ((_variableData._powerState != null) && (_variableData._powerState)) {
            _powerOnTime = System.currentTimeMillis();
         }
         _log.log(Level.FINE, "Creating new VMInfo <%%V%s%%V>(%s) for cluster <%%C%s%%C>. %s. %s",
               new String[]{moRef, moRef, clusterId, _constantData.toString(), _variableData.toString()});
      }

      long _powerOnTime; // most recent timestamp when VHM learned VM is on
   }

   class ClusterInfo {
      final String _masterUUID;
      final SerengetiClusterConstantData _constantData;

      public ClusterInfo(String clusterId, SerengetiClusterConstantData constantData) {
         this._masterUUID = clusterId;
         this._constantData = constantData;
         _completionEvents = new LinkedList<ClusterScaleCompletionEvent>();
         _log.log(Level.FINE, "Creating new ClusterInfo <%%C%s%%C>(%s). %s",
               new String[]{clusterId, clusterId, constantData.toString()});
      }

      Integer _jobTrackerPort;
      String _discoveredFolderName;        /* Note this field is only set by SerengetiLimitEvents */
      String _scaleStrategyKey;
      LinkedList<ClusterScaleCompletionEvent> _completionEvents;
      Long _incompleteSince;
      Map<String, String> _extraInfo;
   }

   private boolean assertHasData(Set<? extends Object> toTest) {
      return (toTest != null && (toTest.size() > 0));
   }

   private boolean assertHasData(Map<? extends Object, ? extends Object> toTest) {
      return (toTest != null && (toTest.keySet().size() > 0));
   }

   private boolean assertHasData(String data) {
      return ((data != null) && !data.trim().isEmpty());
   }

   private ClusterInfo getCluster(String clusterId) {
      return _clusters.get(clusterId);
   }

   private boolean createCluster(String clusterId, SerengetiClusterConstantData constantData) {
      if (getCluster(clusterId) != null) {
         return false;
      }
      if ((clusterId != null) && (constantData != null)) {
         ClusterInfo cluster = new ClusterInfo(clusterId, constantData);
         _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: recording existance of cluster");
         _clusters.put(clusterId, cluster);
      }
      return true;
   }

   private String removeVM(String vmMoRef) {
      VMInfo vmInfo = _vms.get(vmMoRef);
      String clusterId = null;
      if (vmInfo != null) {
         clusterId = vmInfo._clusterId;
         if (vmInfo._constantData._vmType.equals(VmType.MASTER)) {
            _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: removing record of cluster");
            _clusters.remove(clusterId);
         }
         _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: removing record of VM <%V"+vmMoRef+"%V>");
         _vms.remove(vmMoRef);
      }
      dumpState(Level.FINEST);
      return clusterId;
   }

   /* Returns clusterId of the cluster affected or null if no update occurred (possibly an error) */
   /* May also return any implied scale events of the cluster state change */
   public String handleClusterEvent(ClusterStateChangeEvent event, Set<ClusterScaleEvent> impliedScaleEventsResultSet) {
      String clusterId = null;
      if (event instanceof NewVmEvent) {
         return addNewVM((NewVmEvent)event, impliedScaleEventsResultSet);
      } else if (event instanceof VmUpdateEvent) {
         return updateVMState((VmUpdateEvent)event, impliedScaleEventsResultSet);
      } else if (event instanceof ClusterUpdateEvent) {
         return updateClusterState((ClusterUpdateEvent)event, impliedScaleEventsResultSet, false);
      } else if (event instanceof VmRemovedFromClusterEvent) {
         return removeVMFromCluster((VmRemovedFromClusterEvent)event);
      }
      return clusterId;
   }

   private String addNewVM(NewVmEvent event, Set<ClusterScaleEvent> impliedScaleEventsResultSet) {
      VMConstantData constantData = event.getConstantData();
      if (constantData == null) {
         _log.severe("VHM: the data expected to be associated with a discovered or new VM is missing");
         return null;
      }

      VMVariableData variableData = event.getVariableData();      /* Can be null */
      String vmId = event.getVmId();
      String clusterId = event.getClusterId();

      if (clusterId == null) {
         _log.severe("VHM: the cluster id associated with a discovered or new VM must not be null");
         return null;
      }

      if (event instanceof NewMasterVMEvent) {
         SerengetiClusterConstantData clusterConstantData = ((NewMasterVMEvent)event).getClusterConstantData();      /* Should not be null */
         SerengetiClusterVariableData clusterVariableData = ((NewMasterVMEvent)event).getClusterVariableData();      /* Should not be null */
         if (!createCluster(clusterId, clusterConstantData)) {
            _log.severe("<%C"+clusterId+"%C>: cluster already exists in cluster map");
            return null;
         }
         updateClusterVariableData(clusterId, clusterVariableData, impliedScaleEventsResultSet, true);
      }

      VMInfo vi = createNewVM(vmId, constantData, variableData, clusterId);
      if (vi == null) {
         _log.severe("<%C"+clusterId+"%C>: <%V:"+vmId+"%V> - already known VM added a second time as a newly discovered VM");
         return null;
      }

      _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: adding record for VM <%V"+vmId+"%V>");
      return clusterId;
   }

   private VMInfo createNewVM(String vmId, VMConstantData constantData, VMVariableData variableData, String clusterId) {
      if (_vms.get(vmId) != null) {
         return null;
      }
      VMInfo vi = new VMInfo(vmId, constantData, variableData, clusterId);
      _vms.put(vmId, vi);
      return vi;
   }

   private String updateVMState(VmUpdateEvent event, Set<ClusterScaleEvent> impliedScaleEventsResultSet) {
      VMVariableData variableData = event.getVariableData();
      if (event instanceof MasterVmUpdateEvent) {
         String clusterId = getClusterIdForVm(event.getVmId());
         updateClusterVariableData(clusterId, ((MasterVmUpdateEvent)event).getClusterVariableData(),
               impliedScaleEventsResultSet, false);
      }
      return updateVMVariableData(event.getVmId(), variableData);
   }

   private boolean testForUpdate(Object toSet, Object newValue, String id, String fieldName, String prefix, String postfix) {
      if ((newValue != null) && ((toSet == null) || !toSet.equals(newValue))) {
         _log.log(Level.FINE, "Updating %s for %s%s%s to %s", new Object[]{fieldName, prefix, id, postfix, newValue});
         return true;
      }
      return false;
   }

   private boolean testForVMUpdate(Object toSet, Object newValue, String vmId, String fieldName) {
      return testForUpdate(toSet, newValue, vmId, fieldName, "<%V", "%V>");
   }

   private boolean testForClusterUpdate(Object toSet, Object newValue, String clusterId, String fieldName) {
      return testForUpdate(toSet, newValue, clusterId, fieldName, "<%C", "%C>");
   }

   private String updateVMVariableData(String vmId, VMVariableData variableData) {
      VMInfo vi = _vms.get(vmId);
      String clusterId = null;
      if (vi != null) {
         String dnsName = variableData._dnsName;
         String hostMoRef = variableData._hostMoRef;
         String ipAddr = variableData._ipAddr;
         String myName = variableData._myName;
         Boolean powerState = variableData._powerState;
         Integer vCPUs = variableData._vCPUs;
         VMVariableData toSet = vi._variableData;
         if (testForVMUpdate(toSet._dnsName, dnsName, vmId, "dnsName")) {
            toSet._dnsName = dnsName;
         }
         if (testForVMUpdate(toSet._hostMoRef, hostMoRef, vmId, "hostMoRef")) {
            toSet._hostMoRef = hostMoRef;
         }
         if (testForVMUpdate(toSet._ipAddr, ipAddr, vmId, "ipAddr")) {
            toSet._ipAddr = ipAddr;
         }
         if (testForVMUpdate(toSet._myName, myName, vmId, "myName")) {
            toSet._myName = myName;
         }
         if (testForVMUpdate(toSet._powerState, powerState, vmId, "powerState")) {
            toSet._powerState = powerState;
            if (powerState) {
               vi._powerOnTime = System.currentTimeMillis();
            } else {
               vi._powerOnTime = 0;
            }

            if (vi._clusterId != null) {
               _log.log(VhmLevel.USER, "<%C"+vi._clusterId+"%C>: VM <%V"+vmId+"%V> - powered "+(powerState?"on":"off"));
            } else {
               _log.log(VhmLevel.USER, "VM <%V"+vmId+"%V>: powered "+(powerState?"on":"off"));
            }
         }
         if (testForVMUpdate(toSet._vCPUs, vCPUs, vmId, "vCPUs")) {
            toSet._vCPUs = vCPUs;
         }
         if (vi._clusterId != null) {
            clusterId = vi._clusterId;
         }
      }
      return clusterId;
   }

   /* Note that in most cases, the information in variableData will represent deltas - real changes to state.
    * However, this may not always be the case, so this should not be assumed - note testForUpdate methods */
   private void updateClusterVariableData(String clusterId, SerengetiClusterVariableData variableData, Set<ClusterScaleEvent> impliedScaleEventsResultSet, boolean isNewVm) {
      boolean variableDataChanged = false;
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         Boolean enableAutomation = variableData._enableAutomation;
         Integer jobTrackerPort = variableData._jobTrackerPort;
         if (enableAutomation != null) {
            String scaleStrategyKey = _extraInfoMapper.getStrategyKey(variableData, clusterId);
            if (testForClusterUpdate(ci._scaleStrategyKey, scaleStrategyKey, clusterId, "scaleStrategyKey")) {

               _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: cluster scale strategy set to "+scaleStrategyKey);

               ci._scaleStrategyKey = scaleStrategyKey;
               variableDataChanged = true;
            }
         }
         if (testForClusterUpdate(ci._jobTrackerPort, jobTrackerPort, clusterId, "jobTrackerPort")) {
            ci._jobTrackerPort = jobTrackerPort;
            variableDataChanged = true;
         }
         if (ci._extraInfo == null) {
            ci._extraInfo = _extraInfoMapper.parseExtraInfo(variableData, clusterId);
            if (ci._extraInfo != null) {
               _log.fine("Setting extraInfo in <%C"+clusterId+"%C> to "+ci._extraInfo);
               variableDataChanged = true;
            }
         } else {
            Map<String, String> toAdd = _extraInfoMapper.parseExtraInfo(variableData, clusterId);
            if (toAdd != null) {
               if (toAdd != null) {
                  for (String key : toAdd.keySet()) {
                     String newValue = toAdd.get(key);
                     String origValue = ci._extraInfo.get(key);
                     if (testForClusterUpdate(origValue, newValue, clusterId, "extraInfo."+key)) {
                        ci._extraInfo.put(key, newValue);
                        variableDataChanged = true;
                     }
                  }
               }
            }
         }
         /* Don't try to generate implied events for existing clusters that are not viable */
         if (variableDataChanged) {
            Set<ClusterScaleEvent> impliedScaleEvents = _extraInfoMapper.getImpliedScaleEventsForUpdate(variableData, clusterId, isNewVm, isClusterViable(clusterId));
            if ((impliedScaleEvents != null) && (impliedScaleEventsResultSet != null)) {
               impliedScaleEventsResultSet.addAll(impliedScaleEvents);
            }
         }
      }
   }

   /* If the JobTracker is powered off, we may consider the cluster complete, but the cluster is obviously not viable */
   private boolean isClusterViable(String clusterId) {
      if (assertHasData(_clusters) && assertHasData(_vms)) {
         Boolean clusterComplete = validateClusterCompleteness(clusterId, 0);
         if ((clusterComplete == null) || !clusterComplete) {
            return false;
         }
         ClusterInfo ci = _clusters.get(clusterId);
         if (ci != null) {
            VMInfo vi = getMasterVmForCluster(clusterId);
            if ((vi == null) || !vi._variableData._powerState) {
               return false;
            }
         }
         return true;
      }
      return false;
   }

   private VMInfo getMasterVmForCluster(String clusterId) {
      for (VMInfo vmInfo : _vms.values()) {
         if (vmInfo._constantData._vmType.equals(VmType.MASTER)
               && vmInfo._clusterId.equals(clusterId)) {
            return vmInfo;
         }
      }
      return null;
   }

   private String updateClusterState(ClusterUpdateEvent event, Set<ClusterScaleEvent> impliedScaleEventsResultSet, boolean isNewVm) {
      SerengetiClusterVariableData variableData = event.getClusterVariableData();
      String clusterId = getClusterIdForVm(event.getVmId());
      updateClusterVariableData(clusterId, variableData, impliedScaleEventsResultSet, isNewVm);
      return clusterId;
   }

   private String removeVMFromCluster(VmRemovedFromClusterEvent event) {
      return removeVM(event.getVmId());
   }

   public void handleCompletionEvent(ClusterScaleCompletionEvent event) {
      ClusterInfo cluster = getCluster(event.getClusterId());
      if (cluster != null) {
         Set<String> enableVMs = event.getVMsForDecision(ClusterScaleCompletionEvent.ENABLE);
         Set<String> disableVMs = event.getVMsForDecision(ClusterScaleCompletionEvent.DISABLE);
         if (enableVMs != null) {
            _log.log(VhmLevel.USER, "<%C"+event.getClusterId()+"%C>: expansion completed");
         }
         if (disableVMs != null) {
            _log.log(VhmLevel.USER, "<%C"+event.getClusterId()+"%C>: shrinking completed");
         }

         cluster._completionEvents.addFirst(event);
      }
   }

   @Override
   /* Return null if a cluster is not viable as there's no scaling we can do with it */
   public String getScaleStrategyKey(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      ClusterInfo ci = getCluster(clusterId);
      if ((ci != null) && isClusterViable(clusterId)) {
         return ci._scaleStrategyKey;
      }
      return null;
   }

   protected ScaleStrategy getScaleStrategyForCluster(String clusterId) {
      String key = getScaleStrategyKey(clusterId);
      if (key != null) {
         return _scaleStrategies.get(getScaleStrategyKey(clusterId));
      }
      return null;
   }

   protected void registerScaleStrategy(ScaleStrategy strategy) {
      _scaleStrategies.put(strategy.getKey(), strategy);
   }

   @Override
   public Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if ((clusterId != null) && (hostId != null)) {
         return generateComputeVMList(clusterId, hostId, powerState);
      }
      return null;
   }

   private Set<String> generateComputeVMList(final String clusterId, String hostId, Boolean powerState) {
      Set<String> result = new HashSet<String>();
      for (VMInfo vminfo : _vms.values()) {
         try {
            boolean hostTest = (hostId == null) ? true : (hostId.equals(vminfo._variableData._hostMoRef));
            boolean clusterTest = (clusterId == null) ? true : (vminfo._clusterId.equals(clusterId));
            boolean powerStateTest = (powerState == null) ? true : (vminfo._variableData._powerState == powerState);
            _log.finest("Testing "+vminfo._variableData._myName+" h="+hostTest+", c="+clusterTest+", p="+powerStateTest);
            if ((vminfo._constantData._vmType.equals(VmType.COMPUTE)) && hostTest && clusterTest && powerStateTest) {
               result.add(vminfo._moRef);
            }
         } catch (NullPointerException e) {
            /* vmInfo._constantData should never be null or have null values.
             * vmInfo._clusterId and vmInfo._moRef should never be null
             * vmInfo._variableData should never be null, but may have null values */
            VMVariableData variableInfo = vminfo._variableData;
            VMConstantData constantInfo = vminfo._constantData;
            _log.fine("Null pointer checking for matching vm (name: "+variableInfo._myName+
                                                          ", uuid: "+constantInfo._myUUID+
                                                          ", vmType: "+constantInfo._vmType+
                                                          ", host: "+variableInfo._hostMoRef+
                                                          ". powerState: "+variableInfo._powerState+
                                                          ", cluster: <%C"+vminfo._clusterId+
                                                       "%C>, moRef: "+vminfo._moRef+")");
         }
      }
      return (result.size() == 0) ? null : result;
   }

   @Override
   public Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (clusterId != null) {
         return generateComputeVMList(clusterId, null, powerState);
      }
      return null;
   }

   @Override
   public Set<String> listComputeVMsForPowerState(boolean powerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      return generateComputeVMList(null, null, powerState);
   }

   @Override
   public Set<String> listComputeVMsForCluster(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      return generateComputeVMList(clusterId, null, null);
   }

   @Override
   public Set<String> listHostsWithComputeVMsForCluster(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         Set<String> result = new HashSet<String>();
         for (VMInfo vminfo : _vms.values()) {
            if ((vminfo._constantData._vmType.equals(VmType.COMPUTE)) && vminfo._clusterId.equals(clusterId)) {
               String hostMoRef = vminfo._variableData._hostMoRef;
               if (assertHasData(hostMoRef)) {
                  result.add(hostMoRef);
               }
            }
         }
         return (result.size() == 0) ? null : result;
      }
      return null;
   }

   public void dumpState(Level logLevel) {
      for (ClusterInfo ci : _clusters.values()) {
         _log.log(logLevel, "<%C"+ci._masterUUID+"%C>: strategy=" + ci._scaleStrategyKey +
               " extraInfoMap= "+ ci._extraInfo + " uuid= " + ci._masterUUID + " jobTrackerPort= "+ci._jobTrackerPort);
      }

      for (VMInfo vmInfo : _vms.values()) {
         VMVariableData variableData = vmInfo._variableData;
         String powerState = variableData._powerState ? " ON" : " OFF";
         String vCPUs = " vCPUs=" + ((variableData._vCPUs == null) ? "N/A" : variableData._vCPUs);
         String host = (variableData._hostMoRef == null) ? "N/A" : variableData._hostMoRef;
         String masterUUID = (vmInfo._clusterId == null) ? "N/A" : vmInfo._clusterId;
         String role = vmInfo._constantData._vmType.name();
         String ipAddr = (variableData._ipAddr == null) ? "N/A" : variableData._ipAddr;
         String dnsName = (variableData._dnsName == null) ? "N/A" : variableData._dnsName;
         String jtPort = "";
         _log.log(logLevel, "<%C"+masterUUID+"%C>: <%V"+vmInfo._moRef+"%V> - vm state: " + role + powerState + vCPUs +
               " host=" + host + " IP=" + ipAddr + "(" + dnsName + ")" + jtPort);
      }
   }

   void associateFolderWithCluster(String clusterId, String folderName) {
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         ci._discoveredFolderName = folderName;
      }
   }

   String getClusterIdFromVMs(List<String> vms) {
      String clusterId = null;
      if (vms != null) {
         for (String moRef : vms) {
            try {
               clusterId = _vms.get(moRef)._clusterId;
               break;
            } catch (NullPointerException e) {}
         }
      }
      return clusterId;
   }

   /* Returns true, false or null
    * - True == is complete
    * - False == is not complete, but has become incomplete since graceTimeMillis
    * - Null == is not complete and has been not complete for longer than graceTimeMillis - cluster is possibly broken or invalid
    * -      == clusterId not found
    * If graceTimeMillis == 0 this implies infinite time
    */
   Boolean validateClusterCompleteness(String clusterId, long graceTimeMillis) {
      boolean isComplete = true;
      Boolean result = null;
      ClusterInfo ci = getCluster(clusterId);
      if (ci == null) {
         return null;
      }
      if ((ci._jobTrackerPort == null) || (ci._masterUUID == null) || (ci._scaleStrategyKey == null)) {
         isComplete = false;
      }
      if (isComplete) {
         Set<String> computeVMs = listComputeVMsForCluster(clusterId);
         isComplete = ((computeVMs != null) && (computeVMs.size() > 0));
      }
      if (isComplete) {
         if (ci._incompleteSince != null) {
            ci._incompleteSince = null;
         }
         result = true;
      } else {
         long currentTime = System.currentTimeMillis();
         if (ci._incompleteSince == null) {
            ci._incompleteSince = currentTime;
            result = false;
         } else if ((ci._incompleteSince > (currentTime - graceTimeMillis) || (graceTimeMillis == 0))) {
            result = false;
         }
      }
      return result;
   }

   @Override
   public String getClusterIdForFolder(String clusterFolderName) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_clusters)) {
         for (ClusterInfo ci : _clusters.values()) {
            String constantFolder = ci._constantData._serengetiFolder;     /* Set when cluster is created by Serengeti */
            String discoveredFolder = ci._discoveredFolderName;            /* Discovered from SerengetiLimitInstruction */
            if (((constantFolder != null) && (constantFolder.equals(clusterFolderName))) ||
                  ((discoveredFolder != null) && (discoveredFolder.equals(clusterFolderName)))) {
               return ci._masterUUID;
            }
         }
      }
      return null;
   }

   @Override
   public String getHostIdForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         VMInfo vmInfo = _vms.get(vmId);
         if (vmInfo != null) {
            return vmInfo._variableData._hostMoRef;
         }
      }
      return null;
   }

   @Override
   public String getClusterIdForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         VMInfo vmInfo = _vms.get(vmId);
         if (vmInfo != null) {
            return vmInfo._clusterId;
         }
      }
      return null;
   }

   @Override
   public ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_clusters)) {
         ClusterInfo info = getCluster(clusterId);
         if (info != null) {
            if (info._completionEvents.size() > 0) {
               return info._completionEvents.getFirst();
            }
         }
      }
      return null;
   }

   @Override
   public Boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(vmIds) && assertHasData(_vms)) {
         for (String vmId : vmIds) {
            Boolean result = checkPowerStateOfVm(vmId, expectedPowerState);
            if ((result == null) || (result == false)) {
               return result;
            }
         }
         return true;
      }
      return null;
   }

   @Override
   public Boolean checkPowerStateOfVm(String vmId, boolean expectedPowerState) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         VMInfo vm = _vms.get(vmId);
         if (vm != null) {
            if (vm._variableData._powerState == null) {
               return null;
            }
            return vm._variableData._powerState == expectedPowerState;
         } else {
            _log.warning("VHM: <%V"+vmId+"%V> - vm does not exist in cluster map");
            return null;
         }
      }
      return null;
   }

   @Override
   public Map<String, String> getHostIdsForVMs(Set<String> vmIds) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(vmIds) && assertHasData(_vms)) {
         Map<String, String> results = new HashMap<String, String>();
         for (String vmId : vmIds) {
            VMInfo vminfo = _vms.get(vmId);
            if ((vminfo != null) && assertHasData(vminfo._variableData._hostMoRef)) {
               results.put(vmId, vminfo._variableData._hostMoRef);
            }
         }
         if (results.size() > 0) {
            return results;
         }
      }
      return null;
   }

   @Override
   public String[] getAllKnownClusterIds() {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_clusters)) {
         return _clusters.keySet().toArray(new String[0]);
      }
      return null;
   }

   @Override
   /* HadoopClusterInfo returned may contain null values for any of its fields except for clusterId
    * This method will return null if a JobTracker representing the cluster is powered off */
   public HadoopClusterInfo getHadoopInfoForCluster(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_clusters)) {
         ClusterInfo ci = getCluster(clusterId);
         HadoopClusterInfo result = null;
         if (ci != null) {
            VMInfo vi = _vms.get(ci._constantData._masterMoRef);
            Boolean powerState = checkPowerStateOfVm(vi._moRef, true);
            if ((vi != null) && (powerState != null) && powerState) {
               /* Constant and Variable data references are guaranteed to be non-null. iPAddress or dnsName may be null */
               result = new HadoopClusterInfo(ci._masterUUID, vi._variableData._dnsName,
                     vi._variableData._ipAddr, ci._jobTrackerPort);
            }
         }
         return result;
      }
      return null;
   }

   @Override
   /* Note that the map returned will only contain VMs that have got a valid DnsName */
   public Map<String, String> getDnsNamesForVMs(Set<String> vmIds) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(vmIds) && assertHasData(_vms)) {
         Map<String, String> results = new HashMap<String, String>();
         for (String vmId : vmIds) {
            String dnsName = getDnsNameForVM(vmId);
            if (dnsName != null) {
               results.put(vmId, dnsName);
            }
         }
         if (results.size() > 0) {
            return results;
         }
      }
      return null;
   }

   @Override
   public String getDnsNameForVM(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         VMInfo vminfo = _vms.get(vmId);
         if (vminfo != null) {
            String dnsName = vminfo._variableData._dnsName;
            if (assertHasData(dnsName)) {
               return dnsName;
            }
         }
      }
      return null;
   }

   @Override
   public Map<String, String> getVmIdsForDnsNames(Set<String> dnsNames) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(dnsNames) && assertHasData(_vms)) {
         Map<String, String> results = new HashMap<String, String>();
         for (VMInfo vminfo : _vms.values()) {
            String dnsNameToTest = vminfo._variableData._dnsName;
            if (assertHasData(dnsNameToTest) && dnsNames.contains(dnsNameToTest)) {
               results.put(dnsNameToTest, vminfo._moRef);
            }
         }
         if (results.size() > 0) {
            return results;
         }
      }
      return null;
   }

   @Override
   public String getVmIdForDnsName(String dnsName) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         for (VMInfo vminfo : _vms.values()) {
            String dnsNameToTest = vminfo._variableData._dnsName;
            if (assertHasData(dnsNameToTest) && (dnsName != null) && dnsNameToTest.equals(dnsName)) {
               return vminfo._moRef;
            }
         }
      }
      return null;
   }

   @Override
   public Integer getNumVCPUsForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         VMInfo vm = _vms.get(vmId);
         if (vm != null) {
            return vm._variableData._vCPUs;
         }
      }
      return null;
   }

   @Override
   public Long getPowerOnTimeForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_vms)) {
         VMInfo vm = _vms.get(vmId);
         if (vm != null) {
            return vm._powerOnTime;
         }
      }
      return null;
   }

   @Override
   public String getExtraInfo(String clusterId, String key) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      ClusterInfo info = getCluster(clusterId);
      if (info != null) {
         if (info._extraInfo != null) {
            return info._extraInfo.get(key);
         }
      }
      return null;
   }

   @Override
   public String[] getAllClusterIdsForScaleStrategyKey(String key) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (assertHasData(_clusters) && (key != null)) {
         Set<String> result = new HashSet<String>();
         for (String clusterId : _clusters.keySet()) {
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
}
