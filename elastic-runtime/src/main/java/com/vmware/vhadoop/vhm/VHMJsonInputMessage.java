package com.vmware.vhadoop.vhm;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMInputMessage;

public class VHMJsonInputMessage implements VHMInputMessage {
   private static final Logger _log = Logger.getLogger(VHMJsonInputMessage.class.getName());

   private byte[] _data;
   
   // TODO:  move to separate file?
   private class VHMCommandMessage {
      int version;
      String cluster_name;
      String jobtracker;
      int instance_num;
      String[] node_groups;
   }

   private VHMCommandMessage _command; 
   
   public VHMJsonInputMessage(byte[] data) {
      _data = data;
      Gson gson = new Gson();
      String jsonString = new String(data);

      try {
         _command = gson.fromJson(jsonString, VHMCommandMessage.class);

         if (_command.version != 1) {
            _log.log(Level.WARNING, "Unknown version = " + _command.version);
            _command = new VHMCommandMessage();
         }
      } catch (Exception e) {
         _log.log(Level.WARNING, "Json parse error (" + e.getMessage() + ") for message: " + jsonString);
         _command = new VHMCommandMessage();
      }
   }

   @Override
   public byte[] getRawPayload() {
      return _data;
   }
   
   @Override
   public String getClusterName() {
      return _command.cluster_name;
   }
   
   @Override
   public int getTargetTTs() {
      return _command.instance_num;
   }

   @Override
   public String getJobTrackerAddress() {
      return _command.jobtracker;
   }

   @Override
   public String[] getTTFolderNames() {
      return _command.node_groups;
   }

}
