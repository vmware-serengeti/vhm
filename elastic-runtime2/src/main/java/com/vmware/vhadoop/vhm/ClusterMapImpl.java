package com.vmware.vhadoop.vhm;

import java.util.Collections;
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
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.vhm.events.ScaleStrategyChangeEvent;
import com.vmware.vhadoop.vhm.events.VMRemovedFromClusterEvent;
import com.vmware.vhadoop.vhm.events.VMUpdatedEvent;

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
   
   private static final String SERENGETI_MASTERVM_NAME_POSTFIX = "-master";

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
      boolean _isMaster;
      boolean _isElastic;
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

   private void updateVMState(VMEventData vmd) {
      VMInfo vmInfo = _vms.get(vmd._vmMoRef);
      ClusterInfo ci = null;
      
      if (vmInfo == null) {
         vmInfo = new VMInfo(vmd._vmMoRef);
         _vms.put(vmd._vmMoRef, vmInfo);
         _log.log(Level.INFO, "New VM <%V" + vmInfo._moRef);
      }

      /* When a new VM is added, we expect a complete vmd with masterUUID and everything */
      /* If the state of a VM is updated, we only get partial info - almost certainly no masterUUID */
      if (vmd._masterUUID != null) {
         ci = vmInfo._cluster = getCluster(vmd._masterUUID);
      }
      
      /* If we didn't get a masterUUID, we may be able to find the cluster from the VMInfo */
      if (ci == null) {
         ci = vmInfo._cluster;
      }

      if (vmd._hostMoRef != null) {
         vmInfo._host = getHost(vmd._hostMoRef);
      }

      if (vmd._powerState != null) {
         vmInfo._powerState = vmd._powerState;
         if (vmInfo._powerState) {
            vmInfo._powerOnTime = System.currentTimeMillis();
         }
      }
      if (vmd._myName != null) {
         vmInfo._name = vmd._myName;
      }
      if (vmd._vCPUs != null) {
         vmInfo._vCPUs = vmd._vCPUs;
      }
      if (vmd._ipAddr != null) {
         vmInfo._ipAddr = vmd._ipAddr;
      }
      if (vmd._dnsName != null) {
         vmInfo._dnsName = vmd._dnsName;
      }
      if (vmd._myUUID != null) {
         vmInfo._myUUID = vmd._myUUID;
      }
      if (vmd._isElastic != null) {
         vmInfo._isElastic = vmd._isElastic;
      }
      if (vmd._masterVmData != null) {
         if (ci != null) {
            if (ci._masterVM == null) {
               ci._masterVM = vmInfo;
               if (vmInfo._moRef != null) {
                  String masterVmName = ci._masterVM._name;
                  int masterIndex = masterVmName.indexOf(SERENGETI_MASTERVM_NAME_POSTFIX);
                  String clusterName = (masterIndex >= 0) ? masterVmName.substring(0, masterIndex) : masterVmName;
                  LogFormatter._clusterIdToNameMapper.put(ci._masterUUID, clusterName);
               }
            }
            if (vmd._masterVmData._enableAutomation != null) {
               vmInfo._isMaster = true;
               String scaleStrategyKey = _extraInfoMapper.getStrategyKey(vmd);
               ci._scaleStrategyKey = scaleStrategyKey;
            }
            if (ci._extraInfo == null) {
               ci._extraInfo = _extraInfoMapper.parseExtraInfo(vmd);
            } else {
               Map<String, String> toAdd = _extraInfoMapper.parseExtraInfo(vmd);
               if (toAdd != null) {
                  ci._extraInfo.putAll(toAdd);
               }
            }
            if (vmd._serengetiFolder != null) {
               ci._folderName = vmd._serengetiFolder;
            }
            if (vmd._masterVmData._jobTrackerPort != null) {
               ci._jobTrackerPort = vmd._masterVmData._jobTrackerPort;
            }
         } else {
            _log.severe("VMEventData is providing cluster level updates without a masterUUID!");
         }
      }
      dumpState(Level.FINE);
   }

   private void removeCluster(ClusterInfo cluster) {
      if (cluster != null) {
         _log.log(Level.INFO, "Remove cluster " + cluster._masterUUID + " uuid=" + cluster._masterUUID);
         _clusters.remove(cluster._masterUUID);
      }
   }

   private void removeVM(String vmMoRef) {
      VMInfo vmInfo = _vms.get(vmMoRef);
      if (vmInfo != null) {
         if (vmInfo._isMaster) {
            removeCluster(vmInfo._cluster);
         }
         _log.log(Level.INFO, "Remove VM " + vmInfo._name);
         _vms.remove(vmMoRef);
      }
      dumpState(Level.FINE);
   }

   private void changeScaleStrategy(String clusterId, String newStrategyKey) {
      ClusterInfo cluster = _clusters.get(clusterId);
      cluster._scaleStrategyKey = newStrategyKey;
   }

   public void handleClusterEvent(ClusterStateChangeEvent event) {
      if (event instanceof VMUpdatedEvent) {
         VMUpdatedEvent ace = (VMUpdatedEvent)event;
         updateVMState(ace.getVm());
      } else if (event instanceof VMRemovedFromClusterEvent) {
         VMRemovedFromClusterEvent rce = (VMRemovedFromClusterEvent)event;
         removeVM(rce.getVmMoRef());
      } else if (event instanceof ScaleStrategyChangeEvent) {
         ScaleStrategyChangeEvent sce = (ScaleStrategyChangeEvent)event;
         changeScaleStrategy(sce.getClusterId(), sce.getNewStrategyKey());
      }
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
            if ((vminfo._isElastic) && hostTest && clusterTest && powerStateTest) {
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
            if ((vminfo._isElastic) && vminfo._cluster._masterUUID.equals(clusterId)) {
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
         String role = vmInfo._isElastic ? "compute" : "other";
         String ipAddr = (vmInfo._ipAddr == null) ? "N/A" : vmInfo._ipAddr;
         String dnsName = (vmInfo._dnsName == null) ? "N/A" : vmInfo._dnsName;
         String jtPort = "";
         if (vmInfo._isMaster) {
            role = "master";
         }
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
