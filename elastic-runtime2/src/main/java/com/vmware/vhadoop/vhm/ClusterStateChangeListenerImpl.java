package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.VCActions.MasterVmEventData;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterConstantData;
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

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterStateChangeListenerImpl extends AbstractClusterMapReader implements EventProducer {
   private static final Logger _log = Logger.getLogger("ChangeListener");
   private static final int backoffPeriodMS = 60000; // 1 minute in milliseconds

   EventConsumer _eventConsumer;
   VCActions _vcActions;
   String _serengetiFolderName;
   boolean _started;
   HashMap<String, NullData> _interimVMData;
   
   class NullData {
   }

   class CachedVMConstantData extends VMConstantData {
      Boolean _isElastic;
      Boolean _isMaster;
   }
   
   class InterimVmData extends NullData {
      String _clusterId;
      CachedVMConstantData _vmConstantData;
      VMVariableData _vmVariableData;
      SerengetiClusterConstantData _clusterConstantData;
      SerengetiClusterVariableData _clusterVariableData;
   }
   
   public ClusterStateChangeListenerImpl(VCActions vcActions, String serengetiFolderName) {
      _vcActions = vcActions;
      _serengetiFolderName = serengetiFolderName;
      _interimVMData = new HashMap<String, NullData>();
   }
   
   @Override
   public void registerEventConsumer(EventConsumer consumer) {
      _eventConsumer = consumer;
   }
   
   @Override
   public void start() {
      _started = true;
      new Thread(new Runnable() {
         @Override
         public void run() {
            String version = "";
            while (_started) {
               ArrayList<VMEventData> vmDataList = new ArrayList<VMEventData>();
               try {
                  version = _vcActions.waitForPropertyChange(_serengetiFolderName, version, vmDataList);
               } catch (InterruptedException e) {
                  /* Almost certainly means that stop has been called */
                  continue;
               }
               if (vmDataList.isEmpty() && (version.equals(""))) {
                  /*
                   *  No data received from VC so far -- can happen if user hasn't created any VMs yet
                   *  Adding a sleep to reduce spam.
                   */
                  try {
                     Thread.sleep(backoffPeriodMS);
                  } catch (InterruptedException e) {
                     // TODO Auto-generated catch block
                     e.printStackTrace();
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
            _log.info("ClusterStateChangeListener stopping...");
         }}, "ClusterSCL_Poll_Thread").start();
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
      if (rawData._isMaster != null) {
         result._isMaster = rawData._isMaster;
      }
      if ((result._isElastic != null) && (result._isMaster != null)) {
         result._vmType = result._isElastic ? VmType.COMPUTE : (result._isMaster ? VmType.MASTER : VmType.OTHER);
      }
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
         return result;
      }
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
         return result;
      }
      return null;
   }

   private InterimVmData processInterimVmData(String vmId, VMEventData rawData) {
      NullData nullData = _interimVMData.get(vmId);
      InterimVmData interimVmData = null;
      if (nullData == null) {
         interimVmData = new InterimVmData();
         _interimVMData.put(vmId, interimVmData);
      } else if (nullData instanceof InterimVmData) {
         interimVmData = (InterimVmData)nullData;
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
      return interimVmData;
   }

   private void removeInterimVmData(String vmId) {
      _interimVMData.put(vmId, new NullData());
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
         return new VmRemovedFromClusterEvent(vmId);
      }
      
      InterimVmData interimData = processInterimVmData(vmId, rawData);
      
      if (interimData != null) {
         String clusterId = interimData._clusterId;
         VMConstantData vmConstantData = interimData._vmConstantData;
         VMVariableData vmVariableData = interimData._vmVariableData;
         
         if ((vmConstantData != null) && (vmConstantData.isComplete())) {
            if (vmConstantData._vmType.equals(VmType.MASTER)) {
               SerengetiClusterConstantData clusterConstantData = interimData._clusterConstantData;
               SerengetiClusterVariableData clusterVariableData = interimData._clusterVariableData;
   
               if ((clusterConstantData != null) && (clusterConstantData.isComplete()) &&
                     (clusterVariableData != null) && (clusterVariableData.isComplete())) {
                  result = new NewMasterVMEvent(vmId, clusterId, vmConstantData, vmVariableData, clusterConstantData, clusterVariableData);
               }
            } else {
               result = new NewVmEvent(vmId, clusterId, vmConstantData, vmVariableData);
            }
         }
         if (result != null) {
            removeInterimVmData(vmId);
            return result;
         }
      } else {
         VMVariableData vmVariableData = getVmVariableData(rawData, null);
         SerengetiClusterVariableData clusterVariableData = getClusterVariableData(rawData, null);

         if (vmVariableData != null) {
            if (clusterVariableData != null) {
               return new MasterVmUpdateEvent(vmId, vmVariableData, clusterVariableData);
            } else {
               return new VmUpdateEvent(vmId, vmVariableData);
            }
         } else if (clusterVariableData != null) {
            return new ClusterUpdateEvent(vmId, clusterVariableData);
         }
      }
      return null;
   }

   @Override
   public void stop() {
      _started = false;
      _vcActions.interruptWait();
   }
}
