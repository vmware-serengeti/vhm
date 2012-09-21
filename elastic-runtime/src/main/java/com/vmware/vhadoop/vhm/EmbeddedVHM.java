package com.vmware.vhadoop.vhm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.external.HadoopActions;
import com.vmware.vhadoop.external.HadoopCluster;
import com.vmware.vhadoop.external.MQActions;
import com.vmware.vhadoop.external.MQActions.MessagePayload;
import com.vmware.vhadoop.external.VCActionDTOTypes.FolderDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActions;
import com.vmware.vhadoop.external.VHMProcess;
import com.vmware.vhadoop.vhm.edpolicy.EnableDisableTTPolicy;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMChooserAlgorithm;

/**
 * Represents a VHM which can be embedded with Serengeti
 * Delegates TT VM choosing and Enable/Disable behavior to pluggable implementations
 * 
 * @author bcorrie
 *
 */
public class EmbeddedVHM extends VHMProcess {

   private static final Logger _log = Logger.getLogger(EmbeddedVHM.class.getName());

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
   private HadoopActions _hd;
   
   /* TODO: Do we want to be able to change the VHMConfig? */
   public void init(VHMConfig config, VCActions vc, MQActions mq, HadoopActions hd) {
      _config = config;
      _vc = vc;
      _mq = mq;
      _hd = hd;
      _initialized = true;
   }
   
   /* Blocking method */
   public void start() {
      if (_initialized) {
         _running = true;
         while (_running) {
            _log.log(Level.INFO, "Waiting for message");
            VHMInputMessage input = (VHMInputMessage)_mq.blockAndReceive();
            if (((input == null) || (input.getClusterName() == null)) && _running) {
               _log.log(Level.WARNING, "Failed to receive message");
               continue;
            }
            _log.log(Level.INFO, "Processing message...");
            VHMReturnMessage output = setNumTTVMsForCluster(input);
            _mq.sendMessage(output.getRawPayload());
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

   protected VHMReturnMessage setNumTTVMsForCluster(VHMInputMessage input) {
      try {
         HadoopCluster cluster = new HadoopCluster(input.getClusterName(), input.getJobTrackerAddress());
         EnableDisableTTPolicy edPolicy = _config._enableDisablePolicy;
         VMChooserAlgorithm vmChooser = _config._vmChooser;
         
         _log.log(Level.INFO, "Getting folders...");
         FolderDTO rootFolder = _vc.getRootFolder().get();
         FolderDTO serengetiRootFolder = _vc.getFolderForName(rootFolder, input.getSerengetiRootFolder()).get();
         FolderDTO clusterFolder = _vc.getFolderForName(serengetiRootFolder, input.getClusterName()).get();
         VMDTO[] allTTs = getAllTTsForAllHosts(input.getTTFolderNames(), clusterFolder);
         
         TTStatesForHost[] ttStatesForHosts = _config._enableDisablePolicy.getStateForTTs(allTTs);
         int totalEnabled = 0;
         for (TTStatesForHost ttStatesForHost : ttStatesForHosts) {
            totalEnabled += ttStatesForHost.getEnabled().length;
         }
         int delta = (input.getTargetTTs() - totalEnabled);
         _log.log(Level.INFO, "Target TT VMs to enable/disable = "+delta);

         boolean initialSuccess = false;
         boolean completedSuccess = false;
         if (delta > 0) {
            VMDTO[] ttsToEnable = vmChooser.chooseVMsToEnable(ttStatesForHosts, delta);
            /* The expectation is that enableVMs is blocking */
            initialSuccess = edPolicy.enableTTs(ttsToEnable, (ttsToEnable.length + totalEnabled), cluster);
            if (initialSuccess) {
               completedSuccess = edPolicy.testForSuccess(ttsToEnable, true);
            }
         } else if (delta < 0) {
            VMDTO[] ttsToDisable = vmChooser.chooseVMsToDisable(ttStatesForHosts, 0 - delta);
            /* The expectation is that disableVMs is blocking */
            initialSuccess = edPolicy.disableTTs(ttsToDisable, (totalEnabled - ttsToDisable.length), cluster);
            if (initialSuccess) {
               completedSuccess = edPolicy.testForSuccess(ttsToDisable, false);
            }
         } else {
            initialSuccess = true;
            completedSuccess = true;
         }

         if (delta != 0) {
            _log.log(Level.INFO, "Result: Initial Success = "+initialSuccess+" Completed Success = "+completedSuccess);
         }

         // _vc.dropConnection(); // Temporary testing code TODO replace with junit test
         return new VHMJsonReturnMessage(completedSuccess, null);
      } catch (Exception e) {
         _log.log(Level.SEVERE, "Unexpected error in core VHM", e);
         return new VHMJsonReturnMessage(false, e.getMessage());
      }
   }
   
   private VMDTO[] getAllTTsForAllHosts(String[] folderNames, FolderDTO clusterFolder) throws ExecutionException, InterruptedException {
      List<VMDTO> allTTsAllFolders = new ArrayList<VMDTO>();
      for (String ttFolderName : folderNames) {
         FolderDTO ttFolder = _vc.getFolderForName(clusterFolder, ttFolderName).get();
         _log.log(Level.INFO, "Finding TT states for " + ttFolderName);
         /* Note that these VMDTOs should not be cached for long. If they are vMotioned, the host moIds will be incorrect */
         allTTsAllFolders.addAll(Arrays.asList(_vc.listVMsInFolder(ttFolder).get()));
      }
      return allTTsAllFolders.toArray(new VMDTO[0]);
   }
}
