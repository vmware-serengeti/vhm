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

package com.vmware.vhadoop.vhm.rabbit;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;

public class VHMJsonInputMessage {
   private static final Logger _log = Logger.getLogger(VHMJsonInputMessage.class.getName());
   public static final int TARGET_SIZE_UNLIMITED = -1;
   

   private byte[] _data;
   
   // TODO:  move to separate file?
   private class VHMCommandMessage {
      int version;          // currently at version 3 
      String action;        // one of ("SetTarget", "Unlimit", "WaitForManual")
      String cluster_name;  // name of VM folder
      int instance_num;     // number of desired instances (-1 for unlimit)
      String route_key;     // routing key to use for progress and completion update messages 
   }

   private VHMCommandMessage _command; 
   
   public VHMJsonInputMessage(byte[] data) {
      _data = data;
      Gson gson = new Gson();
      String jsonString = new String(data);

      try {
         _command = gson.fromJson(jsonString, VHMCommandMessage.class);
         if (_command.version < 3) {
            _command.action =  SerengetiLimitInstruction.actionSetTarget;
         }
         if (_command.instance_num == TARGET_SIZE_UNLIMITED) {
            _command.action =  SerengetiLimitInstruction.actionUnlimit;
         }

         if ((_command.version < 1) || (_command.version > 3)) {
            _log.log(Level.WARNING, "Unknown version = " + _command.version);
            throw new RuntimeException();
         }
         if (!_command.action.equals(SerengetiLimitInstruction.actionSetTarget) &&
               !_command.action.equals(SerengetiLimitInstruction.actionUnlimit) &&
               !_command.action.equals(SerengetiLimitInstruction.actionWaitForManual)) {
            _log.log(Level.WARNING, "Unknown action = " + _command.action);
            throw new RuntimeException();
         }

      } catch (Exception e) {
         _log.log(Level.WARNING, "Json parse error (" + e.getMessage() + ") for message: " + jsonString);
         _command = new VHMCommandMessage();
      }
   }

   public String getAction() {
      return _command.action;
   }

   public String getClusterId() {
      return _command.cluster_name;
   }

   public int getInstanceNum() {
      return _command.instance_num;
   }

   public String getRouteKey() {
      return _command.route_key;
   }

}
