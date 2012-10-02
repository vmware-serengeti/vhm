/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
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

package com.vmware.vhadoop.adaptor.vc;

import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.adaptor.vc.VCConnection.MoRefAndProps;
import com.vmware.vhadoop.adaptor.vc.VCConnection.VCCredentials;
import com.vmware.vhadoop.adaptor.vc.VCUtils.AsyncArrayResultTransformer;
import com.vmware.vhadoop.adaptor.vc.VCUtils.AsyncResultTransformer;
import com.vmware.vhadoop.adaptor.vc.VCUtils.CastingResultTransformer;
import com.vmware.vhadoop.external.VCActionDTOTypes.FolderDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.HostDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMPowerState;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.TaskInfoState;
import com.vmware.vim25.VirtualMachinePowerState;

/**
 * Represents an implementation of the necessary methods in VCActions to support the VHM
 * Each method returns an asynchronous result which may or may not represent an async call in reality
 * Some async return type encapsulate a wait for a property change to occur
 * All return types are DTO classes designed to abstractly represent the state of a VC entity
 *
 */
public class VCAdaptor extends AbstractVCAdaptor {
   private VCConnection _connection;
   private static final Logger _log = Logger.getLogger(VCAdaptor.class.getName());

   /* These are property keys for finding out properties of a VC object 
    * The keys should only exist within the scope of the VCAdaptor */
   private static final String VC_PROPERTY_HOST_FOR_VM = "runtime.host";
   private static final String VC_PROPERTY_HOSTNAME_OF_GUEST = "guest.hostName";
   private static final String VC_PROPERTY_RUNTIME_POWERSTATE = "runtime.powerState";
   
   private static final String VC_MOREF_TYPE_VM = "VirtualMachine";
   private static final String VC_MOREF_TYPE_FOLDER = "Folder";
   
   private static final String POWER_STATE_VALUE_ON = "POWERED_ON";
   private static final String POWER_STATE_VALUE_OFF = "POWERED_OFF";
   
   public VCAdaptor(VCCredentials credentials) {
      _connection = new VCConnection(credentials);
      _connection.connect();            /* TODO: Could move into init method for better error handling */
   }
   
   @Override
   public boolean testConnection() {
      return _connection.connect();
   }
   
   // Used for testing, forces log out of vc without orderly VHM disconnect.
   public void dropConnection() {
	  _log.log(Level.SEVERE, "Forcing drop of VC connection for testing");
      _connection.disconnect(true);
   }
   
   @Override
   public Future<VMDTO> powerOffVM(VMDTO vm) {
      ManagedObjectReference futureTaskResult = null;
      try {
         futureTaskResult = _connection.getVimPort().powerOffVMTask((ManagedObjectReference)vm._moId);
      } catch (Exception e) {
         return null;
      }
      AsyncResultTransformer<VMDTO> resultsTransformer = new AsyncResultTransformer<VMDTO>(
            _connection.new MoRefAndProps((ManagedObjectReference)vm._moId, vm._name), _connection) {
         @Override
         public VMDTO transform(MoRefAndProps moRefProps) {
            VMDTO newDTO = new VMDTO(moRefProps._name);
            newDTO._moId = moRefProps._moref;
            return newDTO;
         }
      };
      resultsTransformer.setWaitForPropsOnObj(futureTaskResult);
      resultsTransformer.addWaitProperty("info.state", "state", new Object[]{TaskInfoState.SUCCESS, TaskInfoState.ERROR});
      return resultsTransformer;
   }
   
   @Override
   public Future<VMDTO> shutdownGuest(VMDTO vm) {
      try {
    	 // ShutdownGuest does not return task to track; caller is expected to check/ensure power-off.
         _connection.getVimPort().shutdownGuest((ManagedObjectReference)vm._moId);
         return null;
      } catch (Exception e) {
    	 _log.log(Level.SEVERE, "Exception invoking shutdownGuest; switching to powerOffVM");
         return powerOffVM(vm);
      }
   }

   @Override
   public Future<VMDTO> powerOnVM(VMDTO vm) {
      ManagedObjectReference futureTaskResult = null;
      try {
         futureTaskResult = _connection.getVimPort().powerOnVMTask(
               (ManagedObjectReference)vm._moId, null);
      } catch (Exception e) {
         return null;
      }
      AsyncResultTransformer<VMDTO> resultsTransformer = new AsyncResultTransformer<VMDTO>(
            _connection.new MoRefAndProps((ManagedObjectReference)vm._moId, vm._name), _connection) {
         @Override
         public VMDTO transform(MoRefAndProps moRefProps) {
            VMDTO newDTO = new VMDTO(moRefProps._name);
            newDTO._moId = moRefProps._moref;
            return newDTO;
         }
      };
      resultsTransformer.setWaitForPropsOnObj(futureTaskResult);
      resultsTransformer.addWaitProperty("info.state", "state", new Object[]{TaskInfoState.SUCCESS, TaskInfoState.ERROR});
      return resultsTransformer;
   }

   @Override
   public Future<FolderDTO> getFolderForName(FolderDTO rootFolder, final String name) {
      MoRefAndProps result = null;
      if (rootFolder == null) {
         /* TODO: RootFolder is a made-up name here */
         result = _connection.new MoRefAndProps(_connection.getServiceContent().getRootFolder(), "RootFolder");
      } else {
         result = _connection.findObjectFromRoot((ManagedObjectReference)rootFolder._moId, VC_MOREF_TYPE_FOLDER, name, null);
      }
      AsyncResultTransformer<FolderDTO> resultsTransformer = new AsyncResultTransformer<FolderDTO>(result, _connection) {
         @Override
         public FolderDTO transform(MoRefAndProps moRefProps) {
            FolderDTO newDTO = new FolderDTO(moRefProps._name);
            newDTO._moId = moRefProps._moref;
            return newDTO;
         }
      };
      if (result == null) {
    	  _log.log(Level.SEVERE, "Could not find VC folder with name "+name);
    	  return null;
      }
      return resultsTransformer;
   }
   
   @Override
   public Future<VMDTO[]> listVMsInFolder(FolderDTO folder) {
      final String[] propKeys = {VC_PROPERTY_HOST_FOR_VM, VC_PROPERTY_HOSTNAME_OF_GUEST, VC_PROPERTY_RUNTIME_POWERSTATE};
      
      MoRefAndProps[] result = _connection.findObjectsFromRoot(
            (ManagedObjectReference)folder._moId, VC_MOREF_TYPE_VM, propKeys);

      AsyncArrayResultTransformer<VMDTO> resultsTransformer = new AsyncArrayResultTransformer<VMDTO>(result, _connection) {
         @Override
         public VMDTO transform(MoRefAndProps moRefProps) {
            VMDTO vm = new VMDTO(moRefProps._name);
            vm._moId = moRefProps._moref;
            vm.setCurrentStateProperties(moRefProps._properties);
            return vm;
         }
         @Override 
         public VMDTO[] toArray(List<VMDTO> list) {
            return list.toArray(new VMDTO[0]);
         }
      };
      return resultsTransformer;
   }
   
   @Override
   public Future<FolderDTO> getRootFolder() {
      return getFolderForName(null, null);
   }
   
   /* Note that the refresh parameter here represents the clear understanding that the properties 
    * contained within the VMDTO passed in may be stale and could be refreshed if desired */
   @Override
   public Future<VMPowerState> getPowerState(VMDTO vm, boolean refresh) {
      VirtualMachinePowerState powerStateValue;
      if (!refresh) {
         powerStateValue = (VirtualMachinePowerState)vm.getStateProperty(VC_PROPERTY_RUNTIME_POWERSTATE);
      } else {
         /* Assumption that this is blocking */
         powerStateValue = (VirtualMachinePowerState)_connection.refreshStateProperty(
               (ManagedObjectReference)vm._moId, VC_PROPERTY_RUNTIME_POWERSTATE);
         vm.addStateProperty(VC_PROPERTY_RUNTIME_POWERSTATE, powerStateValue);
      } 
      VMPowerState result;
      if (powerStateValue.name().equals(POWER_STATE_VALUE_ON)) {
         result = VMPowerState.POWERED_ON;
      } else if (powerStateValue.name().equals(POWER_STATE_VALUE_OFF)) {
         result = VMPowerState.POWERED_OFF;
      } else {
         result = VMPowerState.UNKNOWN;
      }
      return new CastingResultTransformer<VMPowerState>(result);
   }
   
   @Override 
   public Future<HostDTO> getHostForVM(VMDTO vm, boolean refresh) {
      ManagedObjectReference moref;
      if (!refresh) {
         moref = (ManagedObjectReference)vm.getStateProperty(VC_PROPERTY_HOST_FOR_VM);
      } else {
         /* Assumption that this is blocking */
         moref = (ManagedObjectReference)_connection.refreshStateProperty(
               (ManagedObjectReference)vm._moId, VC_PROPERTY_HOST_FOR_VM);
         vm.addStateProperty(VC_PROPERTY_HOST_FOR_VM, moref);
      }
      HostDTO host = new HostDTO(moref.getValue());
      host._moId = moref;
      return new CastingResultTransformer<HostDTO>(host);
   }
   
   @Override
   public String getVMHostname(VMDTO vm) {
      return (String)vm.getStateProperty(VC_PROPERTY_HOSTNAME_OF_GUEST);
   }
}
