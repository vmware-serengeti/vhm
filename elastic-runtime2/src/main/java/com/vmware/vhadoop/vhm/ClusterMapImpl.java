package com.vmware.vhadoop.vhm;

import java.util.*;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.ScaleStrategy;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.ComputeVMEventData;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.MasterVMEventData;
import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.vhm.events.ScaleStrategyChangeEvent;
import com.vmware.vhadoop.vhm.events.VMAddedToClusterEvent;
import com.vmware.vhadoop.vhm.events.VMHostChangeEvent;
import com.vmware.vhadoop.vhm.events.VMPowerStateChangeEvent;
import com.vmware.vhadoop.vhm.events.VMRemovedFromClusterEvent;

public class ClusterMapImpl implements com.vmware.vhadoop.api.vhm.ClusterMap {
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
      public VMInfo(String moRef, HostInfo host, ClusterInfo cluster, boolean powerState, boolean isMaster) {
         this._moRef = moRef;
         this._host = host;
         this._cluster = cluster;
         this._powerState = powerState;
         this._isMaster = isMaster;
      }
      final boolean _isMaster;
      final String _moRef;
      HostInfo _host;
      final ClusterInfo _cluster;
      boolean _powerState;
   }

   public class ClusterInfo {
      public ClusterInfo(String masterUUID) {
         this._masterUUID = masterUUID;
      }
      final String _masterUUID;
      int _minInstances;
      String _scaleStrategyKey;
   }
   
   private HostInfo getHost(String hostMoRef) {
      HostInfo host = _hosts.get(hostMoRef);
      if (host == null) {
         host = new HostInfo(hostMoRef);
         _hosts.put(hostMoRef, host);
      }
      return host;
   }

   private ClusterInfo getCluster(String masterUUID) {
      ClusterInfo cluster = _clusters.get(masterUUID);
      if (cluster == null) {
         cluster = new ClusterInfo(masterUUID);
         _clusters.put(masterUUID, cluster);
      }
      return cluster;
   }

   private void changePowerState(String vmMoRef, boolean powerState) {
      VMInfo vm = _vms.get(vmMoRef);
      vm._powerState = powerState;
      System.out.println(Thread.currentThread().getName()+": ClusterMap setting power state to "+powerState+" for VM "+vmMoRef);
   }
   
   private void changeHost(String vmMoRef, String hostMoRef) {
      VMInfo vm = _vms.get(vmMoRef);
      vm._host = getHost(hostMoRef);
      System.out.println(Thread.currentThread().getName()+": ClusterMap setting host to "+hostMoRef+" for VM "+vmMoRef);
   }
   
   private void addVMToCluster(VMEventData vmd) {
      ClusterInfo ci = null;
      boolean isMaster = false;
      if (vmd instanceof ComputeVMEventData) {
         ComputeVMEventData cved = (ComputeVMEventData)vmd;
         ci = getCluster(cved._masterUUID);
      } else if (vmd instanceof MasterVMEventData) {
         MasterVMEventData mved = (MasterVMEventData)vmd;
         ci = getCluster(mved._masterUUID);
         ci._minInstances = mved._minInstances;
         ci._scaleStrategyKey = mved._enableAutomation ? "automatic" : "manual";
         isMaster = true;
      }
      HostInfo host = getHost(vmd._hostMoRef);
      _vms.put(vmd._vmMoRef, new VMInfo(vmd._vmMoRef, host, ci, vmd._powerState, isMaster));
   }
   
   private void removeVMFromCluster(String clusterId, String vmMoRef) {
      ClusterInfo cluster = _clusters.get(clusterId);
      VMInfo vm = _vms.get(vmMoRef);
      if (vm._cluster == cluster) {
         _vms.remove(vmMoRef);
      } else {
         throw new RuntimeException("WRONG!");
      }
   }
   
   private void changeScaleStrategy(String clusterId, String newStrategyKey) {
      ClusterInfo cluster = _clusters.get(clusterId);
      cluster._scaleStrategyKey = newStrategyKey;
   }

   public void handleClusterEvent(ClusterStateChangeEvent event) {
      if (event instanceof VMAddedToClusterEvent) {
         VMAddedToClusterEvent ace = (VMAddedToClusterEvent)event;
         addVMToCluster(ace.getVm());
      } else if (event instanceof VMHostChangeEvent) {
         VMHostChangeEvent hce = (VMHostChangeEvent)event;
         changeHost(hce.getVmMoRef(), hce.getHostMoRef());
      } else if (event instanceof VMPowerStateChangeEvent) {
         VMPowerStateChangeEvent pce = (VMPowerStateChangeEvent)event;
         changePowerState(pce.getVmMoRef(), pce.getNewPowerState());
      } else if (event instanceof VMRemovedFromClusterEvent) {
         VMRemovedFromClusterEvent rce = (VMRemovedFromClusterEvent)event;
         removeVMFromCluster(rce.getClusterId(), rce.getVmMoRef());
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
         
         if (masterUUID.equals(clusterId) && !(vminfo._isMaster) &&
               (vminfo._powerState == powerState)) {
            result.add(vminfo._moRef);
         }
      }
      return result;
   }
}
