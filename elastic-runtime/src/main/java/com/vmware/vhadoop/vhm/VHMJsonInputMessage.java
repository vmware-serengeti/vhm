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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMInputMessage;

public class VHMJsonInputMessage implements VHMInputMessage {
   private static final Logger _log = Logger.getLogger(VHMJsonInputMessage.class.getName());

   private byte[] _data;
   
   // TODO:  move to separate file?
   private class VHMCommandMessage {
      int version;          // currently at version 1 
      String cluster_name;  // name of VM folder
      String jobtracker;    // IP address of jobtracker
      int instance_num;     // number of desired instances (-1 for unlimit)
      String[] node_groups; // list of nodegroups (vm folders) on which to apply the setting
      String serengeti_instance; // VM folder for the instance of serengeti that sent the command
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

   @Override
   public String getSerengetiRootFolder() {
      return _command.serengeti_instance;
   }

}
