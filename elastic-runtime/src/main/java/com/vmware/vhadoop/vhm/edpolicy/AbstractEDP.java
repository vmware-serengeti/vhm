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

package com.vmware.vhadoop.vhm.edpolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.external.VCActionDTOTypes.HostDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMPowerState;
import com.vmware.vhadoop.external.VCActions;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ProgressLogger;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public abstract class AbstractEDP implements EnableDisableTTPolicy {
   private VCActions _vc;

   private static final String _className = AbstractEDP.class.getName();
   static final ProgressLogger _pLog = ProgressLogger.getProgressLogger(_className);
   static final Logger _log = _pLog.getLogger();
   
   private static final int shutdownGuestTimeOutMs = 30000;

   public AbstractEDP(VCActions vc) {
      _vc = vc;
   }
   
   private class FuturePowerStateAndVM {
      Future<VMPowerState> _powerState;
      VMDTO _vm;
   }
   
   private class EnabledDisabledVMLists {
      HostDTO _host;
      List<VMDTO> _enabledVMs = new ArrayList<VMDTO>();
      List<VMDTO> _disabledVMs = new ArrayList<VMDTO>();
      
      public EnabledDisabledVMLists(HostDTO host) {
         _host = host;
      }
   }

   @Override
   public TTStatesForHost[] getStateForTTs(VMDTO[] ttVMs) throws Exception {
      Map<String, EnabledDisabledVMLists> results = new HashMap<String, EnabledDisabledVMLists>();

      /* TODO: Currently our VC calls to get host and power state are synchronously 
       * getting cached data, so the parallelism isn't necessary - it could be in future */

      /* Power state calls to VC are done in parallel for speed */
      List<FuturePowerStateAndVM> powerStateTasks = new ArrayList<FuturePowerStateAndVM>();
      for (VMDTO vm : ttVMs) {
         FuturePowerStateAndVM fpsav = new FuturePowerStateAndVM();
         fpsav._powerState = _vc.getPowerState(vm, false);
         fpsav._vm = vm;
         powerStateTasks.add(fpsav);
         
         /* Build a map of hosts ready for the results below */
         HostDTO host = _vc.getHostForVM(vm, false).get();
         if (!results.containsKey(host._name)) {
            /* Assumption: host name is the unique key */
            results.put(host._name, new EnabledDisabledVMLists(host));
         }
      }
      
      /* Then we go back, get the results and divide up the VMs */
      for (FuturePowerStateAndVM state : powerStateTasks) {
         HostDTO host = _vc.getHostForVM(state._vm, false).get();
         VMDTO ttVM = state._vm;
         switch (state._powerState.get()) {
         case POWERED_ON:
            results.get(host._name)._enabledVMs.add(ttVM);
            break;
         case POWERED_OFF:
            results.get(host._name)._disabledVMs.add(ttVM);
            break;
         }
      }
      
      /* Convert the ordered lists and maps into something consumable */
      TTStatesForHost[] returnVal = new TTStatesForHost[results.size()];
      int cntr = 0;
      for (String hostName : results.keySet()) {
         EnabledDisabledVMLists lists = results.get(hostName);
         TTStatesForHost ttStates = new TTStatesForHost(lists._host,
               lists._enabledVMs,
               lists._disabledVMs);
         returnVal[cntr++] = ttStates;
      }
      
      return returnVal;
   }
   
   /* Return value indicates that all succeeded */
   CompoundStatus blockOnVMTaskCompletion(List<Future<VMDTO>> vmTasks) {
      CompoundStatus status = new CompoundStatus("blockOnVMTaskCompletion");
      for (Future<? extends Object> result : vmTasks) {
         try {
            if (result.get() == null) {
               status.registerTaskFailed(false, "null was unexpectedly returned from a future task");
            } else {
               status.registerTaskSucceeded();
            }
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
      return status;
   }

   CompoundStatus powerOnVMs(VMDTO[] toPowerOn) {
      CompoundStatus status = new CompoundStatus("powerOnVMs");
      List<Future<VMDTO>> powerOnTasks = new ArrayList<Future<VMDTO>>();
      for (VMDTO vm : toPowerOn) {
         _log.log(Level.INFO, "Enabling VM "+vm._name+" ...");
         try {
             VMPowerState vmp = getVC().getPowerState(vm, false).get();
             if (!vmp.equals(VMPowerState.POWERED_ON)) {
                 powerOnTasks.add(getVC().powerOnVM(vm));
             }
         } catch (Exception e) {
            String errorMsg = "Problem determining current power state of vm "+vm._name;
        	 _log.log(Level.SEVERE, errorMsg);
        	 status.registerTaskFailed(false, errorMsg);
         }
      }
      _log.log(Level.INFO, "Waiting for completion...");
      if (powerOnTasks.size() > 0) {
          status.addStatus(blockOnVMTaskCompletion(powerOnTasks)); /* Currently blocking */
          status.addStatus(testForPowerState(toPowerOn, true/*->powered-on*/));
      } else {
    	  status.registerTaskSucceeded();
      }
      _log.log(Level.INFO, "Done");
      return status;
   }
   
   CompoundStatus powerOffVMs(VMDTO[] toPowerOff) {
      CompoundStatus status = new CompoundStatus("powerOffVMs");
      List<Future<VMDTO>> powerOffTasks = new ArrayList<Future<VMDTO>>();
      for (VMDTO vm : toPowerOff) {
         _log.log(Level.INFO, "Disabling VM "+vm._name+" ...");
         powerOffTasks.add(getVC().powerOffVM(vm));
      }
      _log.log(Level.INFO, "Waiting for completion...");
      status.addStatus(blockOnVMTaskCompletion(powerOffTasks)); /* Currently blocking */
      status.addStatus(testForPowerState(toPowerOff, false/*->powered-off*/));
      _log.log(Level.INFO, "Done");
      return status;
   }
   
   CompoundStatus shutdownGuests(VMDTO[] toShutDown) {
      CompoundStatus status = new CompoundStatus("shutdownGuests");
	  List<Future<VMDTO>> shutDownTasks = new ArrayList<Future<VMDTO>>();
	  List<VMDTO> shutDownNoTask = new ArrayList<VMDTO>();
	  for (VMDTO vm : toShutDown) {
	      _log.log(Level.INFO, "Disabling VM "+vm._name+" ...");
	      Future<VMDTO> shutDownTask = getVC().shutdownGuest(vm);
	      if (shutDownTask != null) {
	          shutDownTasks.add(shutDownTask);
	      } else {
	    	  shutDownNoTask.add(vm);
	      }
	  }
	  _log.log(Level.INFO, "Waiting for completion...");
	  if (shutDownNoTask.size() > 0) {
		  // For guest shutdowns w/o tasks, check power status after delay time & force off if not off.
		  try {
			  Thread.sleep(shutdownGuestTimeOutMs);
		  } catch (InterruptedException e) {
			  e.printStackTrace();
		  }
		  for (VMDTO vm : shutDownNoTask) {
		      try {
		          VMPowerState vmp = getVC().getPowerState(vm, true).get();
		          if (!vmp.equals(VMPowerState.POWERED_OFF)) {
		        	  _log.log(Level.INFO, "Forcing power-off after guest shutdown of vm "+vm._name);
		              shutDownTasks.add(getVC().powerOffVM(vm));
		          }
		      } catch (Exception e) {
		          String errorMsg = "Problem determining current power state of vm "+vm._name;
		          _log.log(Level.SEVERE, errorMsg);
		          status.registerTaskFailed(false, errorMsg);
		      }
		  }
	  }
	  if (shutDownTasks.size() > 0) {
	      status.addStatus(blockOnVMTaskCompletion(shutDownTasks)); /* Currently blocking */
	      status.addStatus(testForPowerState(toShutDown, false/*->powered-off*/));
	  }
	  _log.log(Level.INFO, "Done");
	  return status;
   }
   
   CompoundStatus testForPowerState(VMDTO[] vms, boolean assertEnabled){
      CompoundStatus status = new CompoundStatus("testForPowerState");
      VMPowerState successState = assertEnabled ? VMPowerState.POWERED_ON : VMPowerState.POWERED_OFF;
      for (VMDTO vm : vms) {
         /* "true" will refresh the powerstate and get() will block on getting the result */
         try {
            VMPowerState vmp = getVC().getPowerState(vm, true).get();
            if (!vmp.equals(successState)) {
               status.registerTaskFailed(false, "VM "+vm._name+" is not the correct power state");
            } else {
               status.registerTaskSucceeded();
            }
         } catch (Exception e) {
            status.registerTaskFailed(false, "Unexpected exception testing power states "+e.getMessage());
         }
      }
      return status;
   }
   
   VCActions getVC() {
      return _vc;
   }
}
