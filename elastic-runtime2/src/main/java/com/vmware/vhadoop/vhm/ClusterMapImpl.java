package com.vmware.vhadoop.vhm;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.events.ScaleStrategyChangeEvent;
import com.vmware.vhadoop.vhm.events.VMUpdatedEvent;
import com.vmware.vhadoop.vhm.events.VMRemovedFromClusterEvent;

public class ClusterMapImpl implements ClusterMap {
   private static final Logger _log = Logger.getLogger(ClusterMap.class.getName());

   Map<String, ClusterInfo> _clusters = new HashMap<String, ClusterInfo>();
   Map<String, HostInfo> _hosts = new HashMap<String, HostInfo>();
   Map<String, VMInfo> _vms = new HashMap<String, VMInfo>();
   Map<String, ScaleStrategy> _scaleStrategies = new HashMap<String, ScaleStrategy>();
   
   public ClusterMapImpl() {
   }
   
   public class HostInfo {
      public HostInfo(String moRef) {
         this._moRef = moRef;
      }
      final String _moRef;
   }
   
   public class VMInfo {
      public VMInfo(String moRef) {
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
      
      // temporary/cache holding fields until cluster object is created
      Integer _cachedMinInstances;
      String _cachedScaleStrategyKey;
   }

   public class ClusterInfo {
      public ClusterInfo(String masterUUID) {
         this._masterUUID = masterUUID;
      }
      final String _masterUUID;
      String _folderName;
      String _name;
      int _minInstances;
      String _scaleStrategyKey;
   }
   
   private HostInfo getHost(String hostMoRef) {
      HostInfo host = _hosts.get(hostMoRef);
      if ((host == null) && (hostMoRef != null)) {
         host = new HostInfo(hostMoRef);
         _hosts.put(hostMoRef, host);
      }
      return host;
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
      if (vmd._isElastic != null) {
         vmInfo._isElastic = vmd._isElastic;
      }
      
      ClusterInfo ci = vmInfo._cluster;
      if (vmd._enableAutomation != null) {
         vmInfo._isMaster = true;
         String scaleStrategyKey = vmd._enableAutomation ? "automatic" : "manual";
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
         ci._name = vmInfo._name; 
      }
      dumpState();
   }
   
   private void removeCluster(ClusterInfo cluster) {
      if (cluster != null) {
         _log.log(Level.INFO, "Remove cluster " + cluster._name + " uuid=" + cluster._masterUUID);
         _clusters.remove(cluster._masterUUID);
      }
   }

   private void removeVM(String vmMoRef) {
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

   public ScaleStrategy getScaleStrategyForCluster(String clusterId) {
      ClusterInfo cluster = _clusters.get(clusterId);
      return _scaleStrategies.get(cluster._scaleStrategyKey);
   }

   public void registerScaleStrategy(String strategyName, ScaleStrategy strategy) {
      _scaleStrategies.put(strategyName, strategy);
   }

   @Override
   public Set<String> listComputeVMsForClusterAndPowerState(String clusterId, boolean powerState) {
      Set<String> result = new HashSet<String>();
      for (VMInfo vminfo : _vms.values()) {
         String masterUUID = vminfo._cluster._masterUUID;
         
         if (masterUUID.equals(clusterId) && (vminfo._isElastic) &&
               (vminfo._powerState == powerState)) {
            result.add(vminfo._moRef);
         }
      }
      return result;
   }
   
   private void dumpState() {
      for (ClusterInfo ci : _clusters.values()) {
         _log.log(Level.INFO, "Cluster " + ci._name + " strategy=" + ci._scaleStrategyKey +
               " min=" + ci._minInstances + " uuid= " + ci._masterUUID);
      }
      
      for (VMInfo vmInfo : _vms.values()) {
         String powerState = vmInfo._powerState ? " ON" : " OFF";
         String host = (vmInfo._host == null) ? "N/A" : vmInfo._host._moRef;
         String cluster = (vmInfo._cluster == null) ? "N/A" : vmInfo._cluster._name;
         String role = vmInfo._isElastic ? "compute" : "other";
         if (vmInfo._isMaster) {
            role = "master";
         }
         _log.log(Level.INFO, "VM " + vmInfo._moRef + "(" + vmInfo._name + ") " + role + powerState + 
               " host=" + host + " cluster=" + cluster);
      }
   }

   String getClusterIdFromVMsInFolder(String folderName, List<String> vms) {
      String clusterId = null;
      for (String moRef : vms) {
         try {
            clusterId = _vms.get(moRef)._cluster._masterUUID;
            _clusters.get(clusterId)._folderName = folderName;
            break;
         } catch (NullPointerException e) {}
      }
      return clusterId;
   }

   @Override
   public String getClusterIdForFolder(String clusterFolderName) {
      for (ClusterInfo ci : _clusters.values()) {
         if (ci._folderName.equals(clusterFolderName)) {
            return ci._masterUUID;
         }
      }
      return null;
   }
   
}
