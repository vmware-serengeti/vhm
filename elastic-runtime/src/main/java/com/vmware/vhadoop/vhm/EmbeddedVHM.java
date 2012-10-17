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

package com.vmware.vhadoop.vhm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.external.HadoopCluster;
import com.vmware.vhadoop.external.MQActions;
import com.vmware.vhadoop.external.MQActions.MessagePayload;
import com.vmware.vhadoop.external.VCActionDTOTypes.FolderDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActions;
import com.vmware.vhadoop.external.VHMProcess;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.CompoundStatus.TaskStatus;
import com.vmware.vhadoop.util.ProgressLogger;
import com.vmware.vhadoop.util.ProgressLogger.ProgressReporter;
import com.vmware.vhadoop.vhm.edpolicy.EnableDisableTTPolicy;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMChooserAlgorithm;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMChooserAlgorithm.VMCAResult;

/**
 * Represents a VHM which can be embedded with Serengeti
 * Delegates TT VM choosing and Enable/Disable behavior to pluggable implementations
 *
 */
public class EmbeddedVHM extends VHMProcess implements ProgressReporter {

   private static final ProgressLogger _pLog = ProgressLogger.getProgressLogger(EmbeddedVHM.class.getName());
   private static final Logger _log = _pLog.getLogger();

   public interface VHMInputMessage extends MessagePayload {
      public String getClusterName();
      public String getSerengetiRootFolder();
      public String getJobTrackerAddress();
      public String[] getTTFolderNames();
      public int getTargetTTs();
   }
   
   public interface VHMReturnMessage extends MessagePayload {
      /* Room for more behavior if we want it */
   }
   
   private boolean _initialized;
   private boolean _running;
   
   private VHMConfig _config;
   private VCActions _vc;
   private MQActions _mq;
   
   private final int UNLIMIT_CMD = -1;
   
   /* TODO: Do we want to be able to change the VHMConfig? */
   public void init(VHMConfig config, VCActions vc, MQActions mq) {
      _config = config;
      _vc = vc;
      _mq = mq;
      ProgressLogger.setProgressReporter(this);
      _initialized = true;
   }
   
   /* Method blocks until/unless message queue issue occurs */
   public void start() {
      if (_initialized) {
         _running = true;
         while (_running) {
            _log.log(Level.INFO, "Waiting for message");
            VHMInputMessage input = (VHMInputMessage)_mq.blockAndReceive();
            if (((input == null) || (input.getClusterName() == null)) && _running) {
               _log.log(Level.SEVERE, "VHM failed to receive message; stopping...");
               _running = false;
               continue;
            }
            
            boolean vcConnection = false;
            try {
               vcConnection = _vc.testConnection();
            } catch (Exception e) {
               _log.log(Level.SEVERE, "VHM got exception trying to connect to vCenter: "+e);
            }
            if (vcConnection == false) {
               reportError("VHM cannot connect to vCenter; check VHM log for details");
               continue;  // Not stopping VHM since problem may be temporary vCenter issue
            }

            _log.log(Level.INFO, "Processing message...");
            setNumTTVMsForCluster(input);
            
//            progressUpdater.verifyCompletionStatus(true);
         }
      } else {
         _log.log(Level.SEVERE, "VHM is not initialized!");
      }
   }
   
   /* Has to be called from a separate thread since start() is blocking */
   public void stop() {
      _running = false;
      _mq.interrupt();
   }
   
   protected VMDTO[] chooseAllTTVMs(TTStatesForHost[] hostAndVMs) {
	  List<VMDTO> toEnable = new ArrayList<VMDTO>();
      for (TTStatesForHost hostAndVM : hostAndVMs) {
          for (VMDTO vm: hostAndVM.getEnabled()) {
              toEnable.add(vm);
          }
          for (VMDTO vm: hostAndVM.getDisabled()) {
              toEnable.add(vm);
          }
       }
	  return toEnable.toArray(new VMDTO[0]);
   }

   protected void setNumTTVMsForCluster(VHMInputMessage input) {
      CompoundStatus thisStatus = new CompoundStatus("setNumTTVMsForCluster");
      CompoundStatus vmChooserStatus = null;
      CompoundStatus edPolicyStatus = null;
      try {
         HadoopCluster cluster = new HadoopCluster(input.getClusterName(), input.getJobTrackerAddress());
         EnableDisableTTPolicy edPolicy = _config._enableDisablePolicy;
         VMChooserAlgorithm vmChooser = _config._vmChooser;
         
         _pLog.registerProgress(10);
         
         _log.log(Level.INFO, "Getting cluster inventory information...");
         VMDTO[] allTTs = getAllTTsForAllHosts(thisStatus, input.getTTFolderNames(),
                 input.getSerengetiRootFolder(), input.getClusterName());
         if ((allTTs == null) || (allTTs.length == 0)) {
            processCompoundStatuses(thisStatus, vmChooserStatus, edPolicyStatus);
            return;
         }
         
         TTStatesForHost[] ttStatesForHosts = _config._enableDisablePolicy.getStateForTTs(allTTs);
         int totalEnabled = 0;
         for (TTStatesForHost ttStatesForHost : ttStatesForHosts) {
            totalEnabled += ttStatesForHost.getEnabled().size();
         }
         int targetTTs = 0;
         if (input.getTargetTTs() == UNLIMIT_CMD) {
        	_log.log(Level.INFO, "Request to unlimit TT VMs");
        	targetTTs = allTTs.length;
         } else {
            targetTTs = input.getTargetTTs();
         }
         int delta = (targetTTs - totalEnabled);
         _log.log(Level.INFO, "Total TT VMs = "+allTTs.length+", total powered-on TT VMs = "+totalEnabled+", target powered-on TT VMs = "+targetTTs);

         _pLog.registerProgress(30);

         if (input.getTargetTTs() == UNLIMIT_CMD) {
        	 VMDTO[] ttsToEnable = chooseAllTTVMs(ttStatesForHosts);
             _pLog.registerProgress(40);
             /* The expectation is that enableVMs is blocking */
             vmChooserStatus = new CompoundStatus("Null VMChooser");        /* Ensure it's not null */
        	 edPolicyStatus = edPolicy.enableTTs(ttsToEnable, ttsToEnable.length, cluster);
             _pLog.registerProgress(90);
         } else if (delta > 0) {
        	_log.log(Level.INFO, "Target TT VMs to enable/disable = "+delta);
        	VMCAResult chooserResult = vmChooser.chooseVMsToEnable(ttStatesForHosts, allTTs.length, delta);
            VMDTO[] ttsToEnable = chooserResult.getChosenVMs();
            vmChooserStatus = chooserResult.getChooserStatus();
            _pLog.registerProgress(40);
            /* The expectation is that enableVMs is blocking */
            edPolicyStatus = edPolicy.enableTTs(ttsToEnable, (ttsToEnable.length + totalEnabled), cluster);
            _pLog.registerProgress(90);
         } else if (delta < 0) {
        	_log.log(Level.INFO, "Target TT VMs to enable/disable = "+delta);
        	VMCAResult chooserResult = vmChooser.chooseVMsToDisable(ttStatesForHosts, allTTs.length, 0 - delta);
            VMDTO[] ttsToDisable = chooserResult.getChosenVMs();
            vmChooserStatus = chooserResult.getChooserStatus();
            _pLog.registerProgress(40);
            /* The expectation is that disableVMs is blocking */
            edPolicyStatus = edPolicy.disableTTs(ttsToDisable, (totalEnabled - ttsToDisable.length), cluster);
            _pLog.registerProgress(90);
         }

      } catch (Exception e) {
         _log.log(Level.SEVERE, "Unexpected error in core VHM: "+e);
         //e.printStackTrace();
         thisStatus.registerTaskFailed(true, e.getMessage());
      }

      processCompoundStatuses(thisStatus, vmChooserStatus, edPolicyStatus);
   }
   
   private void processCompoundStatuses(CompoundStatus vhmStatus, 
         CompoundStatus vmChooserStatus,
         CompoundStatus edPolicyStatus) {
      /* TODO: Look through the results of the important operations and decide what to report back to Serengeti */
      /* TODO: Trivial example for now */
      //if ((vhmStatus.getFatalFailureCount() + vmChooserStatus.getFatalFailureCount() + edPolicyStatus.getFatalFailureCount()) == 0) {
	  if (((vhmStatus == null)?       0 : vhmStatus.getFailedTaskCount()) +
		  ((vmChooserStatus == null)? 0 : vmChooserStatus.getFailedTaskCount()) +
		  ((edPolicyStatus == null)?  0 : edPolicyStatus.getFailedTaskCount()) == 0) {
	     reportCompletion();
      } else {
         TaskStatus firstError = CompoundStatus.getFirstFailure(
               new CompoundStatus[]{vhmStatus, vmChooserStatus, edPolicyStatus});
         if (CompoundStatus.allPowerOpsSucceeded(edPolicyStatus)) {
            reportError(firstError.getMessage() + " however, powering on/off VMs succeeded;");
         } else {
            reportError(firstError.getMessage());
         }
      }
   }

   private VMDTO[] getAllTTsForAllHosts(CompoundStatus vhmStatus, String[] folderNames,
           String serengetiRootFolderName, String vHadoopClusterName) {
      // Get vCenter root folder
      FolderDTO rootFolder = null;
      try {
         rootFolder = _vc.getRootFolder().get();
      } catch (Exception e) {
         _log.log(Level.SEVERE, "Got exception trying to access root folder in vCenter");
      }
      if (rootFolder == null) {
          vhmStatus.registerTaskFailed(false, "Could not access root folder in vCenter");
          return null;
      }
      
      // Get Serengeti deployment folder
      FolderDTO serengetiRootFolder = null;
      try {
         if ((serengetiRootFolderName != null) & (serengetiRootFolderName.length() != 0)) {
              serengetiRootFolder = _vc.getFolderForName(rootFolder, serengetiRootFolderName).get();
         }
      } catch (Exception e) {
         _log.log(Level.SEVERE, "Got exception trying to access serengeti folder "+serengetiRootFolderName+" in vCenter");
      }
      if (serengetiRootFolder == null) {
          vhmStatus.registerTaskFailed(false,
           "Could not access serengeti root folder "+serengetiRootFolderName+" in vCenter");
          return null;
      }
      
      // Get hadoop cluster folder
      FolderDTO clusterFolder = null;
      try {
         if ((vHadoopClusterName != null) & (vHadoopClusterName.length() != 0)) {
              clusterFolder = _vc.getFolderForName(serengetiRootFolder, vHadoopClusterName).get();
         }
      } catch (Exception e) {
         _log.log(Level.SEVERE, "Got exception trying to access hadoop cluster folder "+vHadoopClusterName+" in vCenter");
      }
      if (clusterFolder == null) {
          vhmStatus.registerTaskFailed(false,
           "Could not access hadoop cluster folder "+vHadoopClusterName+" in vCenter");
          return null;
      }
      
      // Walk through specified task tracker folders
      List<VMDTO> allTTsAllFolders = new ArrayList<VMDTO>();
      if (folderNames != null) {
         for (String ttFolderName : folderNames) {
            FolderDTO ttFolder = null;
            if ((ttFolderName != null) & (ttFolderName.length() != 0)) {
               try {
                  ttFolder =  _vc.getFolderForName(clusterFolder, ttFolderName).get();
               } catch (Exception e) {
                  _log.log(Level.SEVERE, "Got exception trying to access task tracker folder "+ttFolderName+" in vCenter");
               }
               if (ttFolder == null) {
                  vhmStatus.registerTaskFailed(false,
                             "Could not access task tracker folder "+ttFolderName+" in vCenter");
               } else {
                  _log.log(Level.INFO, "Finding TT states for " + ttFolderName);
                  /* Note that these VMDTOs should not be cached for long. If they are vMotioned, the host moIds will be incorrect */
                  VMDTO[] vmsInFolder = null;
                  try {
                     vmsInFolder = _vc.listVMsInFolder(ttFolder).get();
                  } catch (Exception e) {
                     _log.log(Level.SEVERE, "Got exception trying to access vm list in task tracker folder "+ttFolderName+" in vCenter");
                  }
                  if (vmsInFolder != null) {
                     allTTsAllFolders.addAll(Arrays.asList(vmsInFolder));
                  }
               }
            }
         }
      }
      if (allTTsAllFolders.size() == 0) {
         vhmStatus.registerTaskFailed(false, "No task trackers found");
      }
      return allTTsAllFolders.toArray(new VMDTO[0]);
   }

   private void sendMessage(VHMJsonReturnMessage msg) {
      if (_mq != null) {
         _mq.sendMessage(msg.getRawPayload());
      }
   }

   @Override
   public void reportProgress(int percentage, String message) {
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(false, false, percentage, 0, null, message);
      sendMessage(msg);
   }

   public void reportError(String message) {
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, false, 100, 0, message, null);
      sendMessage(msg);
   }

   public void reportCompletion() {
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, true, 100, 0, null, null);
      sendMessage(msg);
   }
}
