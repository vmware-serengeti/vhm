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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.VCActions.MasterVmEventData;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMConstantData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VmType;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.api.vhm.events.EventProducer;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.vhm.events.ClusterUpdateEvent;
import com.vmware.vhadoop.vhm.events.MasterVmUpdateEvent;
import com.vmware.vhadoop.vhm.events.NewMasterVMEvent;
import com.vmware.vhadoop.vhm.events.NewVmEvent;
import com.vmware.vhadoop.vhm.events.VmRemovedFromClusterEvent;
import com.vmware.vhadoop.vhm.events.VmUpdateEvent;
import com.vmware.vhadoop.vhm.vc.VcVlsi;

public class ClusterStateChangeListenerImpl extends AbstractClusterMapReader implements EventProducer {
   private static final Logger _log = Logger.getLogger(ClusterStateChangeListenerImpl.class.getName());

   private final int backoffPeriodMS = 5000;

   EventConsumer _eventConsumer;
   VCActions _vcActions;
   String _serengetiFolderName;
   boolean _started;
   HashMap<String, VmCreatedData> _interimVMData;
   Thread _mainThread;

   long _startTime = System.currentTimeMillis();
   boolean _deliberateFailureTriggered = false;

   /* Place-holder that indicates that a VM has been created, so any further data about it will be an update */
   class VmCreatedData {
   }

   class CachedVMConstantData extends VMConstantData {
      Boolean _isElastic;
      String _masterUUID;
      
      protected String getVariableValues() {
         return "isElastic="+_isElastic+", _masterUUID="+_masterUUID;
      }
      
      @Override
      public String toString() {
         return "CachedVMConstantData{"+super.getVariableValues()+", "+getVariableValues()+"}";
      }
   }

   class InterimVmData extends VmCreatedData {
      String _clusterId;
      CachedVMConstantData _vmConstantData;
      VMVariableData _vmVariableData;
      SerengetiClusterConstantData _clusterConstantData;
      SerengetiClusterVariableData _clusterVariableData;
      
      protected String getVariableValues() {
         return "_clusterId="+_clusterId+", _vmConstantData="+_vmConstantData+", _vmVariableData="+_vmVariableData+
               ", _clusterConstantData="+_clusterConstantData+", _clusterVariableData="+_clusterVariableData;
      }
      
      @Override
      public String toString() {
         return "InterimVmData{"+getVariableValues()+"}";
      }
   }

   @SuppressWarnings("unused")
   private void deliberatelyFail(long afterTimeMillis) {
      if (!_deliberateFailureTriggered && (System.currentTimeMillis() > (_startTime + afterTimeMillis))) {
         _deliberateFailureTriggered = true;
         throw new RuntimeException("Deliberate failure!!");
      }
   }

   public ClusterStateChangeListenerImpl(VCActions vcActions, String serengetiFolderName) {
      _vcActions = vcActions;
      _serengetiFolderName = serengetiFolderName;
      _interimVMData = new HashMap<String, VmCreatedData>();
   }

   @Override
   public void registerEventConsumer(EventConsumer consumer) {
      _eventConsumer = consumer;
   }

   @Override
   public void start(final EventProducerStoppingCallback stoppingCallback) {
      _started = true;
      _mainThread = new Thread(new Runnable() {
         @Override
         public void run() {
            String version = "";
            ArrayList<VMEventData> vmDataList = new ArrayList<VMEventData>();
            boolean fatalError = false;
            try {
               _log.info("ClusterStateChangeListener starting...");
               while (_started) {
                  try {
                     /* If version == null, this usually indicates a VC connection failure */
                     version = _vcActions.waitForPropertyChange(_serengetiFolderName, version, vmDataList);
                  } catch (InterruptedException e) {
                     /* Almost certainly means that stop has been called */
                     continue;
                  }
                  processRawVCUpdates(vmDataList, version);
                  vmDataList.clear();
               }
            } catch (Throwable t) {
               _log.log(Level.SEVERE, "Unexpected exception in ClusterStateChangeListener", t);
               fatalError = true;
            }
            _log.info("ClusterStateChangeListener stopping...");
            if (stoppingCallback != null) {
               stoppingCallback.notifyStopping(ClusterStateChangeListenerImpl.this, fatalError);
            }
         }}, "ClusterSCL_Poll_Thread");
      _mainThread.start();
   }

   protected void processRawVCUpdates(ArrayList<VMEventData> vmDataList, String version) {
      if (vmDataList.isEmpty() && ((version == null) || version.equals(""))) {
         try {
            _log.info("Temporarily lost connection to VC... ");
            Thread.sleep(backoffPeriodMS);
         } catch (InterruptedException e) {
            _log.warning("Unexpectedly interrupted waiting for VC");
         }

      } else {
         for (VMEventData vmData : vmDataList) {
            _log.log(Level.INFO, "Detected change in vm <%V" + vmData._vmMoRef + "%V> leaving= " + vmData._isLeaving);
            ClusterStateChangeEvent csce = translateVMEventData(vmData);
            if (csce != null) {
               _log.info("Created new "+csce+" for vm <%V" + vmData._vmMoRef);
               _eventConsumer.placeEventOnQueue(csce);
            }
         }
      }
   }

   /* Map this as early as possible to help our log messages */
   private String mapClusterIdToClusterName(VMConstantData constantData, VMVariableData variableData) {
      if ((constantData != null) && (constantData.isComplete())) {
         if (constantData._vmType.equals(VmType.MASTER)) {
            String masterVmName = null;
            if ((variableData != null) && (variableData._myName != null)) {
               masterVmName = variableData._myName;
               int masterIndex = masterVmName.indexOf(VcVlsi.SERENGETI_MASTERVM_NAME_POSTFIX);
               String clusterName = (masterIndex >= 0) ? masterVmName.substring(0, masterIndex) : masterVmName;
               String clusterId = constantData._myUUID;
               LogFormatter._clusterIdToNameMapper.put(clusterId, clusterName);
               return clusterId;
            }
         }
      }
      return null;
   }

   private CachedVMConstantData getVmConstantData(VMEventData rawData, CachedVMConstantData cachedConstant) {
      CachedVMConstantData result = (cachedConstant != null) ? cachedConstant : new CachedVMConstantData();
      if (rawData._myUUID != null) {
         result._myUUID = rawData._myUUID;
      }
      if (rawData._isElastic != null) {
         result._isElastic = rawData._isElastic;
      }
      if (rawData._masterUUID != null) {
         result._masterUUID = rawData._masterUUID;
      }
      Boolean isMaster = null;
      if ((result._masterUUID != null) && (result._myUUID != null)) {
         isMaster = result._masterUUID.equals(result._myUUID);
      }
      if ((result._isElastic != null) && (isMaster != null)) {
         result._vmType = result._isElastic ? VmType.COMPUTE : (isMaster ? VmType.MASTER : VmType.OTHER);
      }
      _log.fine("Returning new CachedVMConstantData: "+result+"; cachedConstant: "+cachedConstant);
      return result;
   }

   private VMVariableData getVmVariableData(VMEventData rawData, VMVariableData cachedVariable) {
      if ((rawData._dnsName == null) &&
            (rawData._hostMoRef == null) &&
            (rawData._ipAddr == null) &&
            (rawData._myName == null) &&
            (rawData._powerState == null) &&
            (rawData._vCPUs == null)) {
         return cachedVariable;
      }
      VMVariableData result = (cachedVariable != null) ? cachedVariable : new VMVariableData();
      if (rawData._dnsName != null) {
         result._dnsName = rawData._dnsName;
      }
      if (rawData._hostMoRef != null) {
         result._hostMoRef = rawData._hostMoRef;
      }
      if (rawData._ipAddr != null) {
         result._ipAddr = rawData._ipAddr;
      }
      if (rawData._myName != null) {
         result._myName = rawData._myName;
      }
      if (rawData._powerState != null) {
         result._powerState = rawData._powerState;
      }
      if (rawData._vCPUs != null) {
         result._vCPUs = rawData._vCPUs;
      }
      _log.fine("Returning new VMVariableData: "+result+"; cachedVariable: "+cachedVariable);
      return result;
   }

   private SerengetiClusterConstantData getClusterConstantData(VMEventData rawData, SerengetiClusterConstantData cachedConstant) {
      if ((rawData._masterMoRef != null) || (rawData._serengetiFolder != null)) {
         SerengetiClusterConstantData result = (cachedConstant != null) ? cachedConstant : new SerengetiClusterConstantData();
         if (rawData._masterMoRef != null) {
            result._masterMoRef = rawData._masterMoRef;
         }
         if (rawData._serengetiFolder != null) {
            result._serengetiFolder = rawData._serengetiFolder;
         }
         _log.fine("Returning new SerengetiClusterConstantData: "+result+"; cachedConstant: "+cachedConstant);
         return result;
      }
      _log.finest("Returning null. rawData: "+rawData+" ");
      return null;
   }

   private SerengetiClusterVariableData getClusterVariableData(VMEventData rawData, SerengetiClusterVariableData cachedVariable) {
      MasterVmEventData mved = rawData._masterVmData;
      if (mved != null) {
         SerengetiClusterVariableData result = (cachedVariable != null) ? cachedVariable : new SerengetiClusterVariableData();
         if (mved._enableAutomation != null) {
            result._enableAutomation = mved._enableAutomation;
         }
         if (mved._jobTrackerPort != null) {
            result._jobTrackerPort = mved._jobTrackerPort;
         }
         if (mved._minInstances != null) {
            result._minInstances = mved._minInstances;
         }
         _log.fine("Returning new SerengetiClusterVariableData: "+result+"; cachedConstant: "+cachedVariable);
         return result;
      }
      _log.finest("Returning null. rawData: "+rawData+" ");
      return null;
   }

   private InterimVmData processInterimVmData(String vmId, VMEventData rawData) {
      VmCreatedData nullData = _interimVMData.get(vmId);
      InterimVmData interimVmData = null;
      if (nullData == null) {
         /* Otherwise if this is the first time we've heard about this VM, create a new InterimVmData and stash it */
         interimVmData = new InterimVmData();
         _interimVMData.put(vmId, interimVmData);
      } else if (nullData instanceof InterimVmData) {
         /* If this is not the first time we've heard about it, but we already have some interim data, just retrieve what we have and update it */
         interimVmData = (InterimVmData)nullData;
      } else {
         /* Returns null for a VM which the system already knows about - one which doesn't have "interim" data associated with it */
      }
      if (interimVmData != null) {
         interimVmData._vmConstantData = getVmConstantData(rawData, interimVmData._vmConstantData);
         interimVmData._vmVariableData = getVmVariableData(rawData, interimVmData._vmVariableData);
         interimVmData._clusterConstantData = getClusterConstantData(rawData, interimVmData._clusterConstantData);
         interimVmData._clusterVariableData = getClusterVariableData(rawData, interimVmData._clusterVariableData);
         String derivedClusterId = mapClusterIdToClusterName(interimVmData._vmConstantData, interimVmData._vmVariableData);
         if (interimVmData._clusterId == null) {
            if (derivedClusterId != null) {
               interimVmData._clusterId = derivedClusterId;
            } else if (rawData._masterUUID != null) {
               interimVmData._clusterId = rawData._masterUUID;
            }
         }
      }
      _log.finer("Processed interim VM data: "+interimVmData);
      return interimVmData;
   }

   /* Turn the raw data from VcVlsi into a rich event hierarchy.
    * Data may come in from VcVlsi in bits and pieces, particularly when a new cluster is created,
    *   therefore there are clear data-completeness requirements for when we create certain event types
    */
   protected ClusterStateChangeEvent translateVMEventData(VMEventData rawData) {
      boolean vmBeingRemoved = (rawData._isLeaving);  /* Should not be null */
      String vmId = rawData._vmMoRef;                 /* Should not be null */
      ClusterStateChangeEvent result = null;

      _log.finest("Received rawData: "+rawData);

      if (vmBeingRemoved) {
         /* Replace any interim data or place-holder */
         _log.finer("Generating VmRemovedFromClusterEvent for VM <%V"+vmId);
         _interimVMData.remove(vmId);
         return new VmRemovedFromClusterEvent(vmId);
      }

      InterimVmData interimData = processInterimVmData(vmId, rawData);
      /* There is interim data for this VM which may now be enough to generate a NewVmEvent */
      if (interimData != null) {
         String clusterId = interimData._clusterId;
         VMConstantData vmConstantData = interimData._vmConstantData;
         VMVariableData vmVariableData = interimData._vmVariableData;
         /* VMConstantData being complete is enough for us to create a NewVmEvent or NewMasterVmEvent */
         if ((vmConstantData != null) && (vmConstantData.isComplete())) {
            if (vmConstantData._vmType.equals(VmType.MASTER)) {
               SerengetiClusterConstantData clusterConstantData = interimData._clusterConstantData;
               SerengetiClusterVariableData clusterVariableData = interimData._clusterVariableData;

               if ((clusterConstantData != null) && (clusterConstantData.isComplete()) &&
                     (clusterVariableData != null) && (clusterVariableData.isComplete())) {
                  _log.finer("Generating NewMasterVMEvent for VM <%V"+vmId+"%V> in cluster <%C"+clusterId);
                  result = new NewMasterVMEvent(vmId, clusterId, vmConstantData, vmVariableData, clusterConstantData, clusterVariableData);
               }
            } else {
               _log.finer("Generating NewVmEvent for VM <%V"+vmId+"%V> in cluster <%C"+clusterId);
               result = new NewVmEvent(vmId, clusterId, vmConstantData, vmVariableData);
            }
         }
         if (result != null) {
            /* Replace the interim data with a place-holder which indicates that the VM event has now been created */
            _interimVMData.put(vmId, new VmCreatedData());
            return result;
         }
      /* We already know about this VM and this therefore must be an update to its variable state */
      } else {
         VMVariableData vmVariableData = getVmVariableData(rawData, null);
         SerengetiClusterVariableData clusterVariableData = getClusterVariableData(rawData, null);

         if (vmVariableData != null) {
            if (clusterVariableData != null) {
               _log.finer("Generating MasterVmUpdateEvent for VM <%V"+vmId);
               return new MasterVmUpdateEvent(vmId, vmVariableData, clusterVariableData);
            } else {
               _log.finer("Generating VmUpdateEvent for VM <%V"+vmId);
               return new VmUpdateEvent(vmId, vmVariableData);
            }
         } else if (clusterVariableData != null) {
            _log.finer("Generating ClusterUpdateEvent for VM <%V"+vmId);
            return new ClusterUpdateEvent(vmId, clusterVariableData);
         }
      }
      _log.finer("Returning null");
      return null;
   }

   @Override
   public void stop() {
      _started = false;
      _vcActions.interruptWait();
      /* TODO: Need to implement a cleanup on the PropertyFilter being used in waitForUpdates */
   }

   @Override
   public boolean isStopped() {
      if ((_mainThread == null) || (!_mainThread.isAlive())) {
         return true;
      }
      return false;
   }
}
