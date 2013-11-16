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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
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
 * There should be no need for synchronization in this class provided this model is adhered to
 * 
 * Due to the introduction of CachingClusterMapImpl, there are some simple rules that must be adhered to in ClusterMap code
 * 1) _vms and _clusters should never be accessed directly. Only through the get/update methods
 * 2) Any call to getClusterInfoMap or getVMInfoMap must be preceded somewhere in the call stack 
 *      by a call to clusterInfoMapHasData or vmInfoMapHasData which should gate access to the get methods. This is checked.
 * 3) _vms and _clusters have been sub-classed to intercept access to methods which mutate the maps - if removeAll, putAll or other
 *      methods need to be used in future, ensure that these are also intercepted
 * 4) Any changes to VMInfo or ClusterInfo that could modify their state must call the appropriate listener
 * 5) It any fields are added beyond _clusters and _vms containing state required for the computation of any public ClusterMap methods,
 *    the design of CachingClusterMapImpl must be revisited.
 */
public abstract class AbstractClusterMap implements ClusterMap {
   private static final Logger _log = Logger.getLogger(AbstractClusterMap.class.getName());

   private final Map<String, ClusterInfo> _clusters;
   private final Map<String, VMInfo> _vms;
   
   private VMCollectionUpdateListener _vmCollectionUpdateListener;
   private ClusterCollectionUpdateListener _clusterCollectionUpdateListener;
   private final Map<String, ScaleStrategy> _scaleStrategies = new HashMap<String, ScaleStrategy>();
   
   private final ExtraInfoToClusterMapper _extraInfoMapper;
//   private final Random _random = new Random();     /* Uncomment to do random failure testing */
//   private final int FAILURE_FACTOR = 20;

   final Map<String, DataCheck> _dataCheckMap = Collections.synchronizedMap(new HashMap<String, DataCheck>());
   
   class DataCheck {
      boolean _vmInfoMapChecked;
      int _vmInfoMapAccessCount;
      boolean _clusterInfoMapChecked;
      int _clusterInfoMapAccessCount;
   }
   
   Map<String, ClusterInfo> getClusterInfoMap() {
      DataCheck dataCheck = _dataCheckMap.get(Thread.currentThread().getName());
      if ((dataCheck == null) || !dataCheck._clusterInfoMapChecked) {
         _log.severe("ClusterMap is accessing clusterInfoMap without checking for valid data");
      }
      if (dataCheck != null) {
         dataCheck._clusterInfoMapChecked = false;
         dataCheck._clusterInfoMapAccessCount++;
      }
      return _clusters;
   }
   
   Map<String, VMInfo> getVMInfoMap() {
      DataCheck dataCheck = _dataCheckMap.get(Thread.currentThread().getName());
      if ((dataCheck == null) || !dataCheck._vmInfoMapChecked) {
         _log.severe("ClusterMap is accessing vmInfoMap without checking for valid data");
      }
      if (dataCheck != null) {
         dataCheck._vmInfoMapChecked = false;
         dataCheck._vmInfoMapAccessCount++;
      }
      return _vms;
   }
   
   private DataCheck getDataCheck(String threadName) {
      DataCheck dataCheck = _dataCheckMap.get(threadName);
      if (dataCheck == null) {
         dataCheck = new DataCheck();
         _dataCheckMap.put(threadName, dataCheck);
      }
      return dataCheck;
   }
   
   boolean clusterInfoMapHasData() {
      boolean result = assertHasData(_clusters);
      if (result) {
         DataCheck dataCheck = getDataCheck(Thread.currentThread().getName());
         if (dataCheck._clusterInfoMapChecked) {
            _log.severe("ClusterMap previously checked for clusterInfoMap without actually accessing it");
         } else {
            dataCheck._clusterInfoMapChecked = true;
         }
      }
      return result;
   }
   
   boolean vmInfoMapHasData() {
      boolean result = assertHasData(_vms);
      if (result) {
         DataCheck dataCheck = getDataCheck(Thread.currentThread().getName());
         if (dataCheck._vmInfoMapChecked) {
            _log.severe("ClusterMap previously checked for vmInfoMap without actually accessing it");
         } else {
            dataCheck._vmInfoMapChecked = true;
         }
      }
      return result;
   }
   
   private void updateClusterInfoMap(String clusterId, ClusterInfo clusterInfo) {
      if (_clusters != null) {
         _clusters.put(clusterId, clusterInfo);
      }
   }
   
   private void updateVMInfoMap(String vmId, VMInfo vmInfo) {
      if (_vms != null) {
         _vms.put(vmId, vmInfo);
      }
   }
   
   @SuppressWarnings("serial")
   AbstractClusterMap(ExtraInfoToClusterMapper mapper) {
      _extraInfoMapper = mapper;
      _clusters = new HashMap<String, ClusterInfo>() {
         @Override
         public ClusterInfo put(String clusterId, ClusterInfo clusterInfo) {
            ClusterInfo result = super.put(clusterId, clusterInfo);
            notifyClusterCollectionUpdateListener();
            return result;
         }
         @Override
         public ClusterInfo remove(Object key) {
            ClusterInfo result = super.remove(key);
            notifyClusterCollectionUpdateListener();
            return result;
         }
      };
      _vms = new HashMap<String, VMInfo>() {
         @Override
         public VMInfo put(String vmId, VMInfo vmInfo) {
            VMInfo result = super.put(vmId, vmInfo);
            notifyVMCollectionUpdateListener();
            return result;
         }
         @Override
         public VMInfo remove(Object key) {
            VMInfo result = super.remove(key);
            notifyVMCollectionUpdateListener();
            return result;
         }
      };
   }
   
   VMInfo createVMInfo(String moRef, VMConstantData constantData,
         VMVariableData variableData, String clusterId) {
      VMInfo vmInfo = new VMInfo(moRef, constantData, variableData, clusterId);
      _log.log(Level.FINE, "Creating new VMInfo <%%V%s%%V>(%s) for cluster <%%C%s%%C>. %s. %s",
            new String[]{moRef, moRef, clusterId, constantData.toString(), variableData.toString()});
      return vmInfo;
   }
   
   ClusterInfo createClusterInfo(String clusterId, SerengetiClusterConstantData constantData) {
      ClusterInfo clusterInfo = new ClusterInfo(clusterId, constantData);
      _log.log(Level.FINE, "Creating new ClusterInfo <%%C%s%%C>(%s). %s",
            new String[]{clusterId, clusterId, constantData.toString()});
      return clusterInfo;
   }
   
   void setVMCollectionUpdateListener(VMCollectionUpdateListener listener) {
      _vmCollectionUpdateListener = listener;
   }

   void setClusterCollectionUpdateListener(ClusterCollectionUpdateListener listener) {
      _clusterCollectionUpdateListener = listener;
   }

   private void notifyVMCollectionUpdateListener() {
      if (_vmCollectionUpdateListener != null) {
         _vmCollectionUpdateListener.updatingVMCollection();
      }
   }

   private void notifyClusterCollectionUpdateListener() {
      if (_clusterCollectionUpdateListener != null) {
         _clusterCollectionUpdateListener.updatingClusterCollection();
      }
   }

   boolean assertHasData(Set<? extends Object> toTest) {
      return (toTest != null && (toTest.size() > 0));
   }

   private boolean assertHasData(Map<? extends Object, ? extends Object> toTest) {
      return (toTest != null && (toTest.keySet().size() > 0));
   }

   boolean assertHasData(String data) {
      return ((data != null) && !data.trim().isEmpty());
   }

   ClusterInfo getCluster(String clusterId) {
      if ((clusterId != null) && clusterInfoMapHasData()) {
         return getClusterInfoMap().get(clusterId);
      }
      return null;
   }

   private boolean createCluster(String clusterId, SerengetiClusterConstantData constantData) {
      if (getCluster(clusterId) != null) {
         return false;
      }
      if ((clusterId != null) && (constantData != null)) {
         ClusterInfo cluster = createClusterInfo(clusterId, constantData);
         _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: recording existence of cluster");
         updateClusterInfoMap(clusterId, cluster);
      }
      return true;
   }

   private String removeVM(String vmMoRef) {
      if (!vmInfoMapHasData()) {
         return null;
      }
      VMInfo vmInfo = getVMInfoMap().remove(vmMoRef);
      String clusterId = null;
      if (vmInfo != null) {
         _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: removing record of VM <%V"+vmMoRef+"%V>");
         clusterId = vmInfo.getClusterId();
         if (vmInfo.getVmType().equals(VmType.MASTER)) {
            _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: removing record of cluster");
            if (clusterInfoMapHasData()) {
               getClusterInfoMap().remove(clusterId);
            }
         }
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
      if (vmInfoMapHasData() && (getVMInfoMap().get(vmId) != null)) {
         return null;
      }
      VMInfo vi = createVMInfo(vmId, constantData, variableData, clusterId);
      updateVMInfoMap(vmId, vi);
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
      if (!vmInfoMapHasData()) {
         return null;
      }
      VMInfo vi = getVMInfoMap().get(vmId);
      String clusterId = null;
      if (vi != null) {
         String dnsName = variableData._dnsName;
         String hostMoRef = variableData._hostMoRef;
         Map<String, Set<String>> nicAndIpAddressMap = variableData._nicAndIpAddressMap;
         String myName = variableData._myName;
         Boolean powerState = variableData._powerState;
         Integer vCPUs = variableData._vCPUs;
         
         clusterId = vi.getClusterId();
         if (testForVMUpdate(vi.getHostMoRef(), hostMoRef, vmId, "hostMoRef")) {
            vi.setHostMoRef(hostMoRef);
         }
         if (testForVMUpdate(vi.getMyName(), myName, vmId, "myName")) {
            vi.setMyName(myName);
         }
         if (testForVMUpdate(vi.getPowerState(), powerState, vmId, "powerState")) {
            vi.setPowerState(powerState);
            if (powerState) {
               vi.setPowerOffTime(0);
               vi.setPowerOnTime(System.currentTimeMillis());
            } else {
               vi.setPowerOnTime(0);
               vi.setPowerOffTime(System.currentTimeMillis());
            }
            if (clusterId != null) {
               _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: VM <%V"+vmId+"%V> - powered "+(powerState?"on":"off"));
            } else {
               _log.log(VhmLevel.USER, "VM <%V"+vmId+"%V>: powered "+(powerState?"on":"off"));
            }
         }
         if ((vi.getPowerState() != null) && (!vi.getPowerState())) {
            dnsName = "";        /* Any time we know the VM is powered off, remove stale values */
         }
         if (testForVMUpdate(vi.getDnsName(), dnsName, vmId, "dnsName")) {
            vi.setDnsName(dnsName);
         }
         if (testForVMUpdate(vi.getNicAndIpAddressMap(), nicAndIpAddressMap, vmId, "nicAndIpAddressMap")) {
            vi.setNicAndIpAddressMap(nicAndIpAddressMap);
         }
         if (testForVMUpdate(vi.getvCPUs(), vCPUs, vmId, "vCPUs")) {
            vi.setvCPUs(vCPUs);
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
            if (testForClusterUpdate(ci.getScaleStrategyKey(), scaleStrategyKey, clusterId, "scaleStrategyKey")) {

               _log.log(VhmLevel.USER, "<%C"+clusterId+"%C>: cluster scale strategy set to "+scaleStrategyKey);

               ci.setScaleStrategyKey(scaleStrategyKey);
               variableDataChanged = true;
            }
         }
         if (testForClusterUpdate(ci.getJobTrackerPort(), jobTrackerPort, clusterId, "jobTrackerPort")) {
            ci.setJobTrackerPort(jobTrackerPort);
            variableDataChanged = true;
         }
         Map<String, String> extraInfo = _extraInfoMapper.parseExtraInfo(variableData, clusterId);
         if (extraInfo != null) {
            _log.fine("Setting extraInfo in <%C"+clusterId+"%C> to "+extraInfo);
            if (ci.putAllExtraInfo(extraInfo)) {
               variableDataChanged = true;
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
   boolean isClusterViable(String clusterId) {
      Boolean clusterComplete = validateClusterCompleteness(clusterId, 0);
      if ((clusterComplete == null) || !clusterComplete) {
         return false;
      }
      if (clusterInfoMapHasData()) {
         ClusterInfo ci = getClusterInfoMap().get(clusterId);
         if (ci != null) {
            VMInfo vi = getMasterVmForCluster(clusterId);
            if ((vi == null) || !vi.getPowerState()) {
               return false;
            }
         }
         return true;
      }
      return false;
   }

   private VMInfo getMasterVmForCluster(String clusterId) {
      if (vmInfoMapHasData()) {
         for (VMInfo vmInfo : getVMInfoMap().values()) {
            if (vmInfo.getVmType().equals(VmType.MASTER)
                  && vmInfo.getClusterId().equals(clusterId)) {
               return vmInfo;
            }
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

         cluster.addCompletionEvent(event);
      }
   }

   protected ScaleStrategy getScaleStrategyForCluster(String clusterId) {
      String key = getScaleStrategyKey(clusterId);
      if (key != null) {
         return _scaleStrategies.get(key);
      }
      return null;
   }

   protected void registerScaleStrategy(ScaleStrategy strategy) {
      _scaleStrategies.put(strategy.getKey(), strategy);
   }

   Set<String> generateComputeVMList(final String clusterId, String hostId, Boolean powerState) {
      if (vmInfoMapHasData()) {
         Set<String> result = new HashSet<String>();
         for (VMInfo vminfo : getVMInfoMap().values()) {
            try {
               boolean hostTest = (hostId == null) ? true : (hostId.equals(vminfo.getHostMoRef()));
               boolean clusterTest = (clusterId == null) ? true : (vminfo.getClusterId().equals(clusterId));
               boolean powerStateTest = (powerState == null) ? true : (vminfo.getPowerState() == powerState);
               _log.finest("Testing "+vminfo.getMyName()+" h="+hostTest+", c="+clusterTest+", p="+powerStateTest);
               if ((vminfo.getVmType().equals(VmType.COMPUTE)) && hostTest && clusterTest && powerStateTest) {
                  result.add(vminfo.getMoRef());
               }
            } catch (NullPointerException e) {
               /* vmInfo._constantData should never be null or have null values.
                * vmInfo.getClusterId() and vmInfo.getMoRef() should never be null
                * vmInfo._variableData should never be null, but may have null values */
               _log.fine("Null pointer checking for matching vm (name: "+vminfo.getMyName()+
                                                             ", uuid: "+vminfo.getMyUUID()+
                                                             ", vmType: "+vminfo.getVmType()+
                                                             ", host: "+vminfo.getHostMoRef()+
                                                             ". powerState: "+vminfo.getPowerState()+
                                                             ", cluster: <%C"+vminfo.getClusterId()+
                                                          "%C>, moRef: "+vminfo.getMoRef()+")");
            }
         }
         return (result.size() == 0) ? null : result;
      }
      return null;
   }

   public void dumpState(Level logLevel) {
      if (clusterInfoMapHasData()) {
         for (ClusterInfo ci : getClusterInfoMap().values()) {
            _log.log(logLevel, "<%C"+ci.getClusterId()+"%C>: strategy=" + ci.getScaleStrategyKey() +
                  " extraInfoMap= "+ ci.getExtraInfoMapAsString() + " uuid= " + ci.getClusterId() + " jobTrackerPort= "+ci.getJobTrackerPort());
         }
      }

      if (vmInfoMapHasData()) {
         for (VMInfo vmInfo : getVMInfoMap().values()) {
            String powerState = vmInfo.getPowerState() ? " ON" : " OFF";
            String vCPUs = " vCPUs=" + ((vmInfo.getvCPUs() == null) ? "N/A" : vmInfo.getvCPUs());
            String host = (vmInfo.getHostMoRef() == null) ? "N/A" : vmInfo.getHostMoRef();
            String masterUUID = (vmInfo.getClusterId() == null) ? "N/A" : vmInfo.getClusterId();
            String role = vmInfo.getVmType().name();
            Map<String, Set<String>> nicAndIpAddressMap = (vmInfo.getNicAndIpAddressMap() == null) ? null : vmInfo.getNicAndIpAddressMap();
            String dnsName = (vmInfo.getDnsName() == null) ? "N/A" : vmInfo.getDnsName();
            String jtPort = "";
            _log.log(logLevel, "<%C"+masterUUID+"%C>: <%V"+vmInfo.getMoRef()+"%V> - vm state: " + role + powerState + vCPUs +
                  " host=" + host + " nicAndIps=" + nicAndIpAddressMap + "(" + dnsName + ")" + jtPort);
         }
      }
   }

   void associateFolderWithCluster(String clusterId, String folderName) {
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         ci.setDiscoveredFolderName(folderName);
      }
   }

   String getClusterIdFromVMs(List<String> vms) {
      String clusterId = null;
      if (vmInfoMapHasData()) {
         Map<String, VMInfo> vmInfoMap = getVMInfoMap();
         if (vms != null) {
            for (String moRef : vms) {
               try {
                  clusterId = vmInfoMap.get(moRef).getClusterId();
                  break;
               } catch (NullPointerException e) {}
            }
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
      if ((ci.getJobTrackerPort() == null) || (ci.getClusterId() == null) || (ci.getScaleStrategyKey() == null)) {
         isComplete = false;
      }
      if (isComplete) {
         Set<String> computeVMs = listComputeVMsForCluster(clusterId);
         isComplete = ((computeVMs != null) && (computeVMs.size() > 0));
      }
      if (isComplete) {
         ci.resetIsCompleteTime();
         result = true;
      } else if (!ci.testAndSetIsComplete(System.currentTimeMillis(), graceTimeMillis)) {
         result = false;
      }
      return result;
   }

   @Override
   public String getHostIdForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vmInfo = getVMInfoMap().get(vmId);
         if (vmInfo != null) {
            return vmInfo.getHostMoRef();
         }
      }
      return null;
   }

   @Override
   public String getClusterIdForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vmInfo = getVMInfoMap().get(vmId);
         if (vmInfo != null) {
            return vmInfo.getClusterId();
         }
      }
      return null;
   }

   @Override
   public ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         return ci.getMostRecentCompletionEvent();
      }
      return null;
   }

   @Override
   public String[] getAllKnownClusterIds() {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (clusterInfoMapHasData()) {
         return getClusterInfoMap().keySet().toArray(new String[0]);
      }
      return null;
   }

   @Override
   public Integer getNumVCPUsForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vm = getVMInfoMap().get(vmId);
         if (vm != null) {
            return vm.getvCPUs();
         }
      }
      return null;
   }

   @Override
   public Long getPowerOnTimeForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vm = getVMInfoMap().get(vmId);
         if (vm != null) {
            return vm.getPowerOnTime();
         }
      }
      return null;
   }

   @Override
   public Long getPowerOffTimeForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vm = getVMInfoMap().get(vmId);
         if (vm != null) {
            return vm.getPowerOffTime();
         }
      }
      return null;
   }

   @Override
   public String getExtraInfo(String clusterId, String key) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         return ci.getExtraInfoValue(key);
      }
      return null;
   }
   
   @Override
   public Map<String, Set<String>> getNicAndIpAddressesForVm(String vmId) {
      //if ((_random != null) && ((_random.nextInt() % FAILURE_FACTOR) == 0)) {return null;}
      if (vmInfoMapHasData()) {
         VMInfo vm = getVMInfoMap().get(vmId);
         if (vm != null) {
            return vm.getNicAndIpAddressMap();
         }
      }
      return null;
   }
}
