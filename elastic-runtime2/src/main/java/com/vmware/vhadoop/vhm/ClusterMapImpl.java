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
      public VMInfo(String moRef) {
         this._moRef = moRef;
      }
      final String _moRef;
      Integer _vCPUs;
      HostInfo _host;
      ClusterInfo _cluster;
      VmType _vmType;
      boolean _powerState;
      public String _myUUID;
      public String _name;
      String _ipAddr;
      String _dnsName;
      public long _powerOnTime; // most recent timestamp when VHM learned VM is on
   }

   class ClusterInfo {
      public ClusterInfo(String masterUUID) {
         this._masterUUID = masterUUID;
         _completionEvents = new LinkedList<ClusterScaleCompletionEvent>();
      }
      final String _masterUUID;
      Integer _jobTrackerPort;
      String _folderName;        /* Note this field is only set by SerengetiLimitEvents */
      VMInfo _masterVM;
      String _scaleStrategyKey;
      LinkedList<ClusterScaleCompletionEvent> _completionEvents;
      Map<String, String> _extraInfo;
   }

   private HostInfo getHost(String hostMoRef) {
      HostInfo host = _hosts.get(hostMoRef);
      if ((host == null) && (hostMoRef != null)) {
         host = new HostInfo(hostMoRef);
         _hosts.put(hostMoRef, host);
      }
      return host;
   }

   private boolean assertHasData(Set<? extends Object> toTest) {
      return (toTest != null && (toTest.size() > 0));
   }

   private boolean assertHasData(Map<? extends Object, ? extends Object> toTest) {
      return (toTest != null && (toTest.keySet().size() > 0));
   }

   private ClusterInfo getCluster(String masterUUID) {
      ClusterInfo cluster = _clusters.get(masterUUID);
      if ((cluster == null) && (masterUUID != null)) {
         cluster = new ClusterInfo(masterUUID);
         _clusters.put(masterUUID, cluster);
      }
      return cluster;
   }

   private void removeCluster(ClusterInfo cluster) {
      if (cluster != null) {
         _log.log(Level.INFO, "Removing cluster <%C" + cluster._masterUUID);
         _clusters.remove(cluster._masterUUID);
      }
   }

   private String removeVM(String vmMoRef) {
      VMInfo vmInfo = _vms.get(vmMoRef);
      String clusterId = null;
      if (vmInfo != null) {
         if (vmInfo._cluster != null) {
            clusterId = vmInfo._cluster._masterUUID;
         }
         if (vmInfo._vmType.equals(VmType.MASTER)) {
            removeCluster(vmInfo._cluster);
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
      
      VMInfo vi = _vms.get(vmId);
      if (vi != null) {
         if ((vi._myUUID != constantData._myUUID) || (vi._vmType != constantData._vmType)) {
            _log.severe("Data constants for interim VM are inconsistent!");
            return null;
         }
      } else {
         vi = createNewVM(vmId, constantData);
         if (vi == null) {
            _log.severe("VMInfo already exists for new event data!");
            return null;
         }
      }
      
      if (event instanceof NewMasterVMEvent) {
         SerengetiClusterConstantData clusterConstantData = ((NewMasterVMEvent)event).getClusterConstantData();      /* Should not be null */
         SerengetiClusterVariableData clusterVariableData = ((NewMasterVMEvent)event).getClusterVariableData();      /* Should not be null */
         ClusterInfo ci = getCluster(clusterId);
         ci._masterVM = vi;
         ci._folderName = clusterConstantData._serengetiFolder;
         vi._cluster = ci;
         updateClusterVariableData(clusterId, clusterVariableData, impliedScaleEventsResultSet, true);
      } else {
         vi._cluster = getCluster(clusterId);
      }
      updateVMVariableData(vmId, variableData);
      return clusterId;
   }

   private VMInfo createNewVM(String vmId, VMConstantData constantData) {
      if (_vms.get(vmId) != null) {
         return null;
      }
      VMInfo vi = new VMInfo(vmId);
      vi._myUUID = constantData._myUUID;
      vi._vmType = constantData._vmType;
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
   
   private String updateVMVariableData(String vmId, VMVariableData variableData) {
      VMInfo vi = _vms.get(vmId);
      if (vi != null) {
         String dnsName = variableData._dnsName;
         String hostMoRef = variableData._hostMoRef;
         String ipAddr = variableData._ipAddr;
         String myName = variableData._myName;
         Boolean powerState = variableData._powerState;
         Integer vCPUs = variableData._vCPUs;
         if (dnsName != null) {
            vi._dnsName = dnsName;
            _log.fine("Updating dnsName for <%V"+vmId+"%V> to "+dnsName);
         }
         if (hostMoRef != null) {
            vi._host = getHost(hostMoRef);
            _log.fine("Updating host for <%V"+vmId+"%V> to "+hostMoRef);
         }
         if (ipAddr != null) {
            vi._ipAddr = ipAddr;
            _log.fine("Updating ipAddr for <%V"+vmId+"%V> to "+ipAddr);
         }
         if (myName != null) {
            vi._name = myName;
            _log.fine("Updating myName for <%V"+vmId+"%V> to "+myName);
         }
         if (powerState != null) {
            vi._powerState = powerState;
            _log.fine("Updating powerState for <%V"+vmId+"%V> to "+powerState);
         }
         if (vCPUs != null) {
            vi._vCPUs = vCPUs;
            _log.fine("Updating vCPUs for <%V"+vmId+"%V> to "+vCPUs);
         }
         return vi._cluster._masterUUID;
      }
      return null;
   }

   private void updateClusterVariableData(String clusterId, SerengetiClusterVariableData variableData, 
         Set<ClusterScaleEvent> impliedScaleEventsResultSet, boolean isNewVm) {
      ClusterInfo ci = _clusters.get(clusterId);
      if (ci != null) {
         Boolean enableAutomation = variableData._enableAutomation;
         Integer jobTrackerPort = variableData._jobTrackerPort;
         if (enableAutomation != null) {
            String scaleStrategyKey = _extraInfoMapper.getStrategyKey(variableData, clusterId);
            String logVerb = (ci._scaleStrategyKey == null) ? "Setting" : "Switching";
            _log.info(logVerb+" scale strategy in ClusterMap to "+scaleStrategyKey+" for cluster <%C"+clusterId);
            ci._scaleStrategyKey = scaleStrategyKey;
         }
         if (jobTrackerPort != null) {
            ci._jobTrackerPort = jobTrackerPort;
            _log.fine("Updating jobTrackerPort for <%C"+clusterId+"%C> to "+jobTrackerPort);
         }
         if (ci._extraInfo == null) {
            ci._extraInfo = _extraInfoMapper.parseExtraInfo(variableData, clusterId);
            _log.fine("Setting extraInfo in <%C"+clusterId+"%C> to "+ci._extraInfo);
         } else {
            Map<String, String> toAdd = _extraInfoMapper.parseExtraInfo(variableData, clusterId);
            _log.fine("Changing extraInfo in <%C"+clusterId+"%C> to "+toAdd);
            if (toAdd != null) {
               ci._extraInfo.putAll(toAdd);
            }
         }
         Set<ClusterScaleEvent> impliedScaleEvents = _extraInfoMapper.getImpliedScaleEventsForUpdate(variableData, clusterId, isNewVm);
         if ((impliedScaleEvents != null) && (impliedScaleEventsResultSet != null)) {
            impliedScaleEventsResultSet.addAll(impliedScaleEvents);
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
      cluster._completionEvents.addFirst(event);
   }

   @Override
   public String getScaleStrategyKey(String clusterId) {
      ClusterInfo ci = _clusters.get(clusterId);
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

   private Set<String> generateComputeVMList(final String clusterId, String hostId, boolean powerState) {
      Set<String> result = new HashSet<String>();
      for (VMInfo vminfo : _vms.values()) {
         try {
            boolean hostTest = (hostId == null) ? true : (hostId.equals(vminfo._host._moRef));
            boolean clusterTest = (clusterId == null) ? true : (vminfo._cluster._masterUUID.equals(clusterId));
            boolean powerStateTest = (vminfo._powerState == powerState);
            if ((vminfo._vmType.equals(VmType.COMPUTE)) && hostTest && clusterTest && powerStateTest) {
               result.add(vminfo._moRef);
            }
         } catch (NullPointerException e) {
          //vminfo.xxx could be null for VMs where we only have partial data from VC
          _log.fine("Null pointer checking for matching vm (name: "+vminfo._name+
                                                          ", uuid: "+vminfo._myUUID+
                                                          ", host: "+vminfo._host+
                                                          ", cluster: "+vminfo._cluster+
                                                          ", moRef: "+vminfo._moRef+")");
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
   public Set<String> listHostsWithComputeVMsForCluster(String clusterId) {
      if (assertHasData(_vms)) {
         Set<String> result = new HashSet<String>();
         for (VMInfo vminfo : _vms.values()) {
            if ((vminfo._vmType.equals(VmType.COMPUTE)) && vminfo._cluster._masterUUID.equals(clusterId)) {
               result.add(vminfo._host._moRef);
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
         String powerState = vmInfo._powerState ? " ON" : " OFF";
         String vCPUs = " vCPUs=" + ((vmInfo._vCPUs == null) ? "N/A" : vmInfo._vCPUs);
         String host = (vmInfo._host == null) ? "N/A" : vmInfo._host._moRef;
         String masterUUID = (vmInfo._cluster == null) ? null : vmInfo._cluster._masterUUID;
         String role = vmInfo._vmType.name();
         String ipAddr = (vmInfo._ipAddr == null) ? "N/A" : vmInfo._ipAddr;
         String dnsName = (vmInfo._dnsName == null) ? "N/A" : vmInfo._dnsName;
         String jtPort = "";
         _log.log(logLevel, "VM <%V" + vmInfo._moRef + "%V> " + role + powerState + vCPUs +
               " host=" + host + " cluster=<%C" + masterUUID + "%C> IP=" + ipAddr + "(" + dnsName + ")" + jtPort);
      }
   }

   void associateFolderWithCluster(String clusterId, String folderName) {
      ClusterInfo ci = _clusters.get(clusterId);
      ci._folderName = folderName;
   }

   String getClusterIdFromVMs(List<String> vms) {
      String clusterId = null;
      if (vms != null) {
         for (String moRef : vms) {
            try {
               clusterId = _vms.get(moRef)._cluster._masterUUID;
               break;
            } catch (NullPointerException e) {}
         }
      }
      return clusterId;
   }

   @Override
   public String getClusterIdForFolder(String clusterFolderName) {
      for (ClusterInfo ci : _clusters.values()) {
         //JG: Note that since foldername is only set by Serengeti Limit commands, foldername
         //    for other clusters will be null, which needs to be ignored...
         if (ci._folderName != null && ci._folderName.equals(clusterFolderName)) {
            return ci._masterUUID;
         }
      }
      return null;
   }

   @Override
   public String getHostIdForVm(String vmId) {
      VMInfo vmInfo = _vms.get(vmId);
      if (vmInfo != null) {
         return vmInfo._host._moRef;
      }
      return null;
   }

   @Override
   public String getClusterIdForVm(String vmId) {
      VMInfo vmInfo = _vms.get(vmId);
      if (vmInfo != null) {
         return vmInfo._cluster._masterUUID;
      }
      return null;
   }

   @Override
   public ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(String clusterId) {
      ClusterInfo info =  getCluster(clusterId);
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
               if (vm._powerState != expectedPowerState) {
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
               results.put(vmId, vm._host._moRef);
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
         ClusterInfo ci = _clusters.get(clusterId);
         HadoopClusterInfo result = null;
         if ((ci != null) && (ci._masterVM != null)) {
            result = new HadoopClusterInfo(ci._masterUUID, ci._masterVM._ipAddr, ci._jobTrackerPort);
         }
         return result;
      }
      return null;
   }

   @Override
   public Set<String> getDnsNameForVMs(Set<String> vmIds) {
      if (assertHasData(vmIds) && assertHasData(_vms)) {
         Set<String> results = new HashSet<String>();
         for (String vm : vmIds) {
            VMInfo vminfo = _vms.get(vm);
            if (vminfo != null) {
               results.add(vminfo._dnsName);
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
            return vm._vCPUs;
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
      ClusterInfo info =  getCluster(clusterId);
      if (info != null) {
         if (info._extraInfo != null) {
            return info._extraInfo.get(key);
         }
      }
      return null;
   }
}
