package com.vmware.vhadoop.vhm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

   Map<String, ClusterInfo> _clusters = new HashMap<String, ClusterInfo>();
   Map<String, HostInfo> _hosts = new HashMap<String, HostInfo>();
   Map<String, VMInfo> _vms = new HashMap<String, VMInfo>();
   Map<String, ScaleStrategy> _scaleStrategies = new HashMap<String, ScaleStrategy>();

   final ExtraInfoToClusterMapper _extraInfoMapper;

   public ClusterMapImpl(ExtraInfoToClusterMapper mapper) {
      _extraInfoMapper = mapper;
   }

   class HostInfo {
      public HostInfo(String moRef) {
         this._moRef = moRef;
      }
      final String _moRef;
   }

   class VMInfo {
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
      Map<String, String> _extraInfo;
   }

   private boolean assertHasData(Set<? extends Object> toTest) {
      return (toTest != null && (toTest.size() > 0));
   }

   private boolean assertHasData(Map<? extends Object, ? extends Object> toTest) {
      return (toTest != null && (toTest.keySet().size() > 0));
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
            _log.log(Level.INFO, "Removing Cluster <%C" + clusterId);
            _clusters.remove(clusterId);
         }
         _log.log(Level.INFO, "Removing VM <%V" + vmMoRef);
         _vms.remove(vmMoRef);
      }
      dumpState(Level.FINEST);
      return clusterId;
   }

   /* Returns clusterId of the cluster affected */
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
         _log.severe("VMConstantData cannot be null!");
         return null;
      }

      VMVariableData variableData = event.getVariableData();      /* Can be null */
      String vmId = event.getVmId();
      String clusterId = event.getClusterId();

      if (clusterId == null) {
         _log.severe("ClusterId should not be null!");
         return null;
      }

      if (event instanceof NewMasterVMEvent) {
         SerengetiClusterConstantData clusterConstantData = ((NewMasterVMEvent)event).getClusterConstantData();      /* Should not be null */
         SerengetiClusterVariableData clusterVariableData = ((NewMasterVMEvent)event).getClusterVariableData();      /* Should not be null */
         if (!createCluster(clusterId, clusterConstantData)) {
            _log.severe("Cluster <%C"+clusterId+"%C> already exists!");
            return null;
         }
         updateClusterVariableData(clusterId, clusterVariableData, impliedScaleEventsResultSet, true);
      }

      VMInfo vi = createNewVM(vmId, constantData, variableData, clusterId);
      if (vi == null) {
         _log.severe("VMInfo already exists for new event data!");
         return null;
      }

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

   private void updateClusterVariableData(String clusterId, SerengetiClusterVariableData variableData,
         Set<ClusterScaleEvent> impliedScaleEventsResultSet, boolean isNewVm) {
      boolean variableDataChanged = false;
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         Boolean enableAutomation = variableData._enableAutomation;
         Integer jobTrackerPort = variableData._jobTrackerPort;
         if (enableAutomation != null) {
            String scaleStrategyKey = _extraInfoMapper.getStrategyKey(variableData, clusterId);
            if (testForClusterUpdate(ci._scaleStrategyKey, scaleStrategyKey, clusterId, "scaleStrategyKey")) {
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
         if (variableDataChanged) {
            Set<ClusterScaleEvent> impliedScaleEvents = _extraInfoMapper.getImpliedScaleEventsForUpdate(variableData, clusterId, isNewVm);
            if ((impliedScaleEvents != null) && (impliedScaleEventsResultSet != null)) {
               impliedScaleEventsResultSet.addAll(impliedScaleEvents);
            }
         }
      }
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
         cluster._completionEvents.addFirst(event);
      }
   }

   @Override
   public String getScaleStrategyKey(String clusterId) {
      ClusterInfo ci = getCluster(clusterId);
      if (ci != null) {
         return ci._scaleStrategyKey;
      }
      return null;
   }

   public ScaleStrategy getScaleStrategyForCluster(String clusterId) {
      return _scaleStrategies.get(getScaleStrategyKey(clusterId));
   }

   public void registerScaleStrategy(ScaleStrategy strategy) {
      _scaleStrategies.put(strategy.getKey(), strategy);
   }

   @Override
   public Set<String> listComputeVMsForClusterHostAndPowerState(String clusterId, String hostId, boolean powerState) {
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
      if (clusterId != null) {
         return generateComputeVMList(clusterId, null, powerState);
      }
      return null;
   }

   @Override
   public Set<String> listComputeVMsForPowerState(boolean powerState) {
      return generateComputeVMList(null, null, powerState);
   }

   @Override
   public Set<String> listComputeVMsForCluster(String clusterId) {
      return generateComputeVMList(clusterId, null, null);
   }

   @Override
   public Set<String> listHostsWithComputeVMsForCluster(String clusterId) {
      if (assertHasData(_vms)) {
         Set<String> result = new HashSet<String>();
         for (VMInfo vminfo : _vms.values()) {
            if ((vminfo._constantData._vmType.equals(VmType.COMPUTE)) && vminfo._clusterId.equals(clusterId)) {
               result.add(vminfo._variableData._hostMoRef);
            }
         }
         return (result.size() == 0) ? null : result;
      }
      return null;
   }

   public void dumpState(Level logLevel) {
      for (ClusterInfo ci : _clusters.values()) {
         _log.log(logLevel, "Cluster <%C" + ci._masterUUID + "%C> strategy=" + ci._scaleStrategyKey +
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
         _log.log(logLevel, "VM <%V" + vmInfo._moRef + "%V> " + role + powerState + vCPUs +
               " host=" + host + " cluster=<%C" + masterUUID + "%C> IP=" + ipAddr + "(" + dnsName + ")" + jtPort);
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

   boolean validateClusterCompleteness(String clusterId) {
      ClusterInfo ci = getCluster(clusterId);
      if ((ci == null) || (ci._jobTrackerPort == null) || (ci._masterUUID == null) || (ci._scaleStrategyKey == null)) {
         return false;
      }
      Set<String> computeVMs = listComputeVMsForCluster(clusterId);
      return ((computeVMs != null) && (computeVMs.size() > 0));
   }

   @Override
   public String getClusterIdForFolder(String clusterFolderName) {
      for (ClusterInfo ci : _clusters.values()) {
         String constantFolder = ci._constantData._serengetiFolder;     /* Set when cluster is created by Serengeti */
         String discoveredFolder = ci._discoveredFolderName;            /* Discovered from SerengetiLimitInstruction */
         if (((constantFolder != null) && (constantFolder.equals(clusterFolderName))) ||
               ((discoveredFolder != null) && (discoveredFolder.equals(clusterFolderName)))) {
            return ci._masterUUID;
         }
      }
      return null;
   }

   @Override
   public String getHostIdForVm(String vmId) {
      VMInfo vmInfo = _vms.get(vmId);
      if (vmInfo != null) {
         return vmInfo._variableData._hostMoRef;
      }
      return null;
   }

   @Override
   public String getClusterIdForVm(String vmId) {
      VMInfo vmInfo = _vms.get(vmId);
      if (vmInfo != null) {
         return vmInfo._clusterId;
      }
      return null;
   }

   @Override
   public ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId) {
      ClusterInfo info = getCluster(clusterId);
      if (info != null) {
         if (info._completionEvents.size() > 0) {
            return info._completionEvents.getFirst();
         }
      }
      return null;
   }

   @Override
   public Boolean checkPowerStateOfVms(Set<String> vmIds, boolean expectedPowerState) {
      if (assertHasData(vmIds) && assertHasData(_vms)) {
         for (String vmId : vmIds) {
            VMInfo vm = _vms.get(vmId);
            if (vm != null) {
               if (vm._variableData._powerState != expectedPowerState) {
                  return false;
               }
            } else {
               _log.warning("VM "+vmId+" does not exist in ClusterMap!");
               return null;
            }
         }
         return true;
      }
      return null;
   }

   @Override
   public Map<String, String> getHostIdsForVMs(Set<String> vmIds) {
      if (assertHasData(vmIds) && assertHasData(_vms)) {
         Map<String, String> results = new HashMap<String, String>();
         for (String vmId : vmIds) {
            VMInfo vm = _vms.get(vmId);
            if (vm != null) {
               results.put(vmId, vm._variableData._hostMoRef);
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
      if (assertHasData(_clusters)) {
         return _clusters.keySet().toArray(new String[0]);
      }
      return null;
   }

   @Override
   public HadoopClusterInfo getHadoopInfoForCluster(String clusterId) {
      if (assertHasData(_clusters)) {
         ClusterInfo ci = getCluster(clusterId);
         HadoopClusterInfo result = null;
         if (ci != null) {
            VMInfo vi = _vms.get(ci._constantData._masterMoRef);
            if (vi != null) {
               result = new HadoopClusterInfo(ci._masterUUID, vi._variableData._ipAddr, ci._jobTrackerPort);
            }
         }
         return result;
      }
      return null;
   }

   @Override
   public Map<String, String> getDnsNameForVMs(Set<String> vmIds) {
      if (assertHasData(vmIds) && assertHasData(_vms)) {
         Map<String, String> results = new HashMap<String, String>();
         for (String vm : vmIds) {
            VMInfo vminfo = _vms.get(vm);
            if (vminfo != null) {
               results.put(vm, vminfo._variableData._dnsName);
            }
         }
         if (results.size() > 0) {
            return results;
         }
      }
      return null;
   }

   @Override
   public Integer getNumVCPUsForVm(String vmId) {
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
      if (assertHasData(_clusters) && (key != null)) {
         Set<String> result = new HashSet<String>();
         for (String clusterId : _clusters.keySet()) {
            ClusterInfo ci = getCluster(clusterId);
            if ((ci != null) && (ci._scaleStrategyKey != null) && (ci._scaleStrategyKey.equals(key))) {
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
