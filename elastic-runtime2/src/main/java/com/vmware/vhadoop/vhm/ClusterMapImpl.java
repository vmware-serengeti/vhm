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
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
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

   final ExtraInfoToScaleStrategyMapper _strategyMapper;

   public ClusterMapImpl(final ExtraInfoToScaleStrategyMapper mapper) {
      _strategyMapper = mapper;
   }

   class HostInfo {
      public HostInfo(final String moRef) {
         this._moRef = moRef;
      }
      final String _moRef;
   }

   class VMInfo {
      public VMInfo(final String moRef) {
         this._moRef = moRef;
      }
      final String _moRef;
      HostInfo _host;
      ClusterInfo _cluster;
      boolean _isMaster;
      boolean _isElastic;
      boolean _powerState;
      public String _myUUID;
      public String _name;
      String _ipAddr;
      String _dnsName;
      Integer _jobTrackerPort;

      // temporary/cache holding fields until cluster object is created
      Integer _cachedMinInstances;
      String _cachedScaleStrategyKey;
   }

   class ClusterInfo {
      public ClusterInfo(final String masterUUID) {
         this._masterUUID = masterUUID;
         _completionEvents = new LinkedList<ClusterScaleCompletionEvent>();
      }
      final String _masterUUID;
      String _folderName;        /* Note this field is only set by SerengetiLimitEvents */
      VMInfo _masterVM;
      int _minInstances;
      String _scaleStrategyKey;
      LinkedList<ClusterScaleCompletionEvent> _completionEvents;
   }

   private HostInfo getHost(final String hostMoRef) {
      HostInfo host = _hosts.get(hostMoRef);
      if ((host == null) && (hostMoRef != null)) {
         host = new HostInfo(hostMoRef);
         _hosts.put(hostMoRef, host);
      }
      return host;
   }

   private ClusterInfo getCluster(final String masterUUID) {
      ClusterInfo cluster = _clusters.get(masterUUID);
      if ((cluster == null) && (masterUUID != null)) {
         cluster = new ClusterInfo(masterUUID);
         _clusters.put(masterUUID, cluster);
      }
      return cluster;
   }

   private void updateVMState(final VMEventData vmd) {
      VMInfo vmInfo = _vms.get(vmd._vmMoRef);
      if (vmInfo == null) {
         vmInfo = new VMInfo(vmd._vmMoRef);
         _vms.put(vmd._vmMoRef, vmInfo);
         _log.log(Level.INFO, "New VM " + vmInfo._moRef);
      }

      if (vmd._hostMoRef != null) {
         vmInfo._host = getHost(vmd._hostMoRef);
      }
      if (vmd._masterUUID != null) {
         vmInfo._cluster = getCluster(vmd._masterUUID);
         if (vmInfo._cachedMinInstances != null) {
            vmInfo._cluster._minInstances = vmInfo._cachedMinInstances;
         }
         if (vmInfo._cachedScaleStrategyKey != null) {
            vmInfo._cluster._scaleStrategyKey = vmInfo._cachedScaleStrategyKey;
         }
      }
      if (vmd._powerState != null) {
         vmInfo._powerState = vmd._powerState;
      }
      if (vmd._myName != null) {
         vmInfo._name = vmd._myName;
      }
      if (vmd._myUUID != null) {
         vmInfo._myUUID = vmd._myUUID;
      }
      if (vmd._ipAddr != null) {
         vmInfo._ipAddr = vmd._ipAddr;
      }
      if (vmd._dnsName != null) {
         vmInfo._dnsName = vmd._dnsName;
      }
      if (vmd._jobTrackerPort != null) {
         vmInfo._jobTrackerPort = vmd._jobTrackerPort;
      }
      if (vmd._isElastic != null) {
         vmInfo._isElastic = vmd._isElastic;
      }

      ClusterInfo ci = vmInfo._cluster;
      if (vmd._enableAutomation != null) {
         vmInfo._isMaster = true;
         String scaleStrategyKey = _strategyMapper.getStrategyKey(vmd);
         if (ci != null) {
            ci._scaleStrategyKey = scaleStrategyKey;
         } else {
            vmInfo._cachedScaleStrategyKey = scaleStrategyKey;
         }
      }
      if (vmd._minInstances != null) {
         vmInfo._isMaster = true;
         if (ci != null) {
            ci._minInstances = vmd._minInstances;
         } else {
            vmInfo._cachedMinInstances = vmd._minInstances;
         }
      }
      if ((vmInfo._myUUID != null) && (ci != null) && (ci._masterUUID == vmInfo._myUUID)) {
         vmInfo._isMaster = true;
      }
      if (vmInfo._isMaster && (ci != null)) {
         ci._masterVM = vmInfo;
      }
      dumpState();
   }

   private void removeCluster(final ClusterInfo cluster) {
      if (cluster != null) {
         _log.log(Level.INFO, "Remove cluster " + cluster._masterUUID + " uuid=" + cluster._masterUUID);
         _clusters.remove(cluster._masterUUID);
      }
   }

   private void removeVM(final String vmMoRef) {
      VMInfo vmInfo = _vms.get(vmMoRef);
      if (vmInfo != null) {
         if (vmInfo._isMaster) {
            removeCluster(vmInfo._cluster);
         }
         _log.log(Level.INFO, "Remove VM " + vmInfo._moRef);
         _vms.remove(vmMoRef);
      }
      dumpState();
   }

   private void changeScaleStrategy(final String clusterId, final String newStrategyKey) {
      ClusterInfo cluster = _clusters.get(clusterId);
      cluster._scaleStrategyKey = newStrategyKey;
   }

   public void handleClusterEvent(final ClusterStateChangeEvent event) {
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

   public void handleCompletionEvent(final ClusterScaleCompletionEvent event) {
      ClusterInfo cluster = getCluster(event.getClusterId());
      cluster._completionEvents.addFirst(event);
   }

   public ScaleStrategy getScaleStrategyForCluster(final String clusterId) {
      ClusterInfo cluster = _clusters.get(clusterId);
      return _scaleStrategies.get(cluster._scaleStrategyKey);
   }

   public void registerScaleStrategy(final ScaleStrategy strategy) {
      _scaleStrategies.put(strategy.getName(), strategy);
   }

   @Override
   public Set<String> listComputeVMsForClusterHostAndPowerState(
         final String clusterId, final String hostId, final boolean powerState) {
      Set<String> result = new HashSet<String>();
      for (VMInfo vminfo : _vms.values()) {
         try {
            boolean hostTest = (hostId == null) ? true : (hostId.equals(vminfo._host._moRef));
            boolean clusterTest = (clusterId == null) ? true : (vminfo._cluster._masterUUID.equals(clusterId));
            boolean powerStateTest = (vminfo._powerState == powerState);
            if ((vminfo._isElastic) && hostTest && clusterTest && powerStateTest) {
               result.add(vminfo._moRef);
            }
         } catch (NullPointerException e) {} //vminfo.xxx could be null for VMs where we only have partial data from VC
      }
      return result;
   }

   @Override
   public Set<String> listComputeVMsForClusterAndPowerState(final String clusterId, final boolean powerState) {
      return listComputeVMsForClusterHostAndPowerState(clusterId, null, powerState);
   }

   @Override
   public Set<String> listComputeVMsForPowerState(final boolean powerState) {
      return listComputeVMsForClusterHostAndPowerState(null, null, powerState);
   }

   @Override
   public Set<String> listHostsWithComputeVMsForCluster(final String clusterId) {
      Set<String> result = new HashSet<String>();
      for (VMInfo vminfo : _vms.values()) {
         if ((vminfo._isElastic) && vminfo._cluster._masterUUID.equals(clusterId)) {
            result.add(vminfo._host._moRef);
         }
      }
      return result;
   }

   private void dumpState() {
      for (ClusterInfo ci : _clusters.values()) {
         String clusterName = (ci._masterVM == null) ? "N/A" : ci._masterVM._name;
         _log.log(Level.FINE, "Cluster " + clusterName + " strategy=" + ci._scaleStrategyKey +
               " min=" + ci._minInstances + " uuid= " + ci._masterUUID);
      }

      for (VMInfo vmInfo : _vms.values()) {
         String powerState = vmInfo._powerState ? " ON" : " OFF";
         String host = (vmInfo._host == null) ? "N/A" : vmInfo._host._moRef;
         VMInfo masterVM = (vmInfo._cluster == null) ? null : vmInfo._cluster._masterVM;
         String cluster = (masterVM == null) ? "N/A" : masterVM._name;
         String role = vmInfo._isElastic ? "compute" : "other";
         String ipAddr = (vmInfo._ipAddr == null) ? "N/A" : vmInfo._ipAddr;
         String dnsName = (vmInfo._dnsName == null) ? "N/A" : vmInfo._dnsName;
         String jtPort = "";
         if (vmInfo._isMaster) {
            role = "master";
            if (vmInfo._jobTrackerPort != null) {
               jtPort = " JTport=" + vmInfo._jobTrackerPort;
            }
         }
         _log.log(Level.FINE, "VM " + vmInfo._moRef + "(" + vmInfo._name + ") " + role + powerState +
               " host=" + host + " cluster=" + cluster + " IP=" + ipAddr + "(" + dnsName + ")" + jtPort);
      }
   }

   String getClusterIdFromVMsInFolder(final String folderName, final List<String> vms) {
      String clusterId = null;
      for (String moRef : vms) {
         try {
            clusterId = _vms.get(moRef)._cluster._masterUUID;
            getCluster(clusterId)._folderName = folderName;
            break;
         } catch (NullPointerException e) {}
      }
      return clusterId;
   }

   @Override
   public String getClusterIdForFolder(final String clusterFolderName) {
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
   public String getHostIdForVm(final String vmId) {
      return _vms.get(vmId)._host._moRef;
   }

   @Override
   public String getClusterIdForVm(final String vmId) {
      return _vms.get(vmId)._cluster._masterUUID;
   }

   @Override
   public ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(final String clusterId) {
      ClusterInfo info =  getCluster(clusterId);
      if (info._completionEvents.size() > 0) {
         return info._completionEvents.getFirst();
      }
      return null;
   }

   @Override
   public boolean checkPowerStateOfVms(final Set<String> vmIds, final boolean expectedPowerState) {
      for (String vmId : vmIds) {
         VMInfo vm = _vms.get(vmId);
         if (vmId != null) {
            if (vm._powerState != expectedPowerState) {
               return false;
            }
         } else {
            _log.warning("VM "+vmId+" does not exist in ClusterMap!");
         }
      }
      return true;
   }

   @Override
   public Map<String, String> getHostIdsForVMs(final Set<String> vms) {
      Map<String, String> result = new HashMap<String, String>();
      for (String vmId : vms) {
         VMInfo vm = _vms.get(vmId);
         if (vm != null) {
            result.put(vmId, vm._host._moRef);
         }
      }
      return result;
   }

   @Override
   public String[] getAllKnownClusterIds() {
      if (_clusters != null) {
         return _clusters.keySet().toArray(new String[0]);
      }
      return null;
   }

   @Override
   public String getScaleStrategyKey(final String clusterId) {
      return _clusters.get(clusterId)._scaleStrategyKey;
   }

   @Override
   public HadoopClusterInfo getHadoopInfoForCluster(final String clusterId) {
      ClusterInfo ci = _clusters.get(clusterId);
      HadoopClusterInfo result = null;
      if (ci != null) {
         result = new HadoopClusterInfo(ci._masterUUID, ci._masterVM._ipAddr);
      }
      return result;
   }

   @Override
   public Set<String> getDnsNameForVMs(final Set<String> vms) {
      Set<String> results = new HashSet<String>();
      for (String vm : vms) {
         VMInfo vminfo = _vms.get(vm);
         if (vminfo != null) {
            results.add(vminfo._dnsName);
         }
      }
      return results;
   }

}
