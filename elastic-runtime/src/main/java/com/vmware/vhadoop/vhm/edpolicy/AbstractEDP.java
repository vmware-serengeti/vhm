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
import com.vmware.vhadoop.vhm.TTStatesForHost;

public abstract class AbstractEDP implements EnableDisableTTPolicy {
   private VCActions _vc;
   static final Logger _log = Logger.getLogger(AbstractEDP.class.getName());

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
   boolean blockOnVMTaskCompletion(List<Future<VMDTO>> vmTasks) {
      boolean allSucceeded = true;
      for (Future<? extends Object> result : vmTasks) {
         try {
            if (result.get() == null) {
               allSucceeded = false;
            }
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
      return allSucceeded;
   }

   boolean powerOnVMs(VMDTO[] toPowerOn) {
      boolean allSucceeded;
      List<Future<VMDTO>> powerOnTasks = new ArrayList<Future<VMDTO>>();
      for (VMDTO vm : toPowerOn) {
         _log.log(Level.INFO, "Enabling VM "+vm._name+" ...");
         powerOnTasks.add(getVC().powerOnVM(vm));
      }
      _log.log(Level.INFO, "Waiting for completion...");
      allSucceeded = blockOnVMTaskCompletion(powerOnTasks); /* Currently blocking */
      _log.log(Level.INFO, "Done");
      return allSucceeded;
   }
   
   boolean powerOffVMs(VMDTO[] toPowerOff) {
      boolean allSucceeded;
      List<Future<VMDTO>> powerOffTasks = new ArrayList<Future<VMDTO>>();
      for (VMDTO vm : toPowerOff) {
         _log.log(Level.INFO, "Disabling VM "+vm._name+" ...");
         powerOffTasks.add(getVC().powerOffVM(vm));
      }
      _log.log(Level.INFO, "Waiting for completion...");
      allSucceeded = blockOnVMTaskCompletion(powerOffTasks); /* Currently blocking */
      _log.log(Level.INFO, "Done");
      return allSucceeded;
   }
   
   boolean testForPowerState(VMDTO[] vms, boolean assertEnabled){
      VMPowerState successState = assertEnabled ? VMPowerState.POWERED_ON : VMPowerState.POWERED_OFF;
      for (VMDTO vm : vms) {
         /* "true" will refresh the powerstate and get() will block on getting the result */
         try {
            VMPowerState vmp = getVC().getPowerState(vm, true).get();
            if (!vmp.equals(successState)) {
               return false;
            }
         } catch (Exception e) {
            return false;
         }
      }
      return true;
   }
   
   VCActions getVC() {
      return _vc;
   }
}
