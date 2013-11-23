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

package com.vmware.vhadoop.vhm.events;

import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.QueueClient;
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;

public class SerengetiLimitInstruction extends AbstractClusterScaleEvent {
   public enum SerengetiLimitAction {
      actionSetTarget("SetTarget"),
      actionUnlimit("Unlimit"),
      actionWaitForManual("WaitForManual");
      
      String _value;
      private SerengetiLimitAction(String value) {
         _value = value;
      }
      
      @Override
      public String toString() {
         return _value;
      }
   };
   
   private static final String reason = "serengeti limit instruction";

   private final SerengetiLimitAction _action;
   private final String _clusterName;
   private final int _toSize;
   private final QueueClient _messageCallback;

   private static final Logger _log = Logger.getLogger(SerengetiLimitInstruction.class.getName());

   public SerengetiLimitInstruction(String clusterName, SerengetiLimitAction action, int toSize, QueueClient messageCallback) {
      super(reason);
      _action = action;
      _clusterName = clusterName;
      _toSize = toSize;
      _messageCallback = messageCallback;
   }

   public SerengetiLimitAction getAction() {
      return _action;
   }

   /* This is the cluster name, not the cluster ID */
   public String getClusterName() {
      return _clusterName;
   }

   public int getToSize() {
      return _toSize;
   }

   public void acknowledgeReceipt() {
      if (_messageCallback != null) {
         _log.info("Acknowledging receipt of instruction");
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(false, false, 0, 0, null, "limit instruction received by VHM");
         /* Note RouteKey is encaspulated in messageCallback */
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   public void reportProgress(int percentage, String message) {
      if (_messageCallback != null) {
         _log.info("Reporting progress "+percentage+"%");
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(false, false, percentage, 0, null, message);
         /* Note RouteKey is encaspulated in messageCallback */
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   public void reportError(String message) {
      if (_messageCallback != null) {
         _log.warning(_clusterName+" - error while attempting to "+toString()+" - "+message+";");
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, false, 100, 0, message, null);
         /* Note RouteKey is encaspulated in messageCallback */
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   public void reportCompletion() {
      if (_messageCallback != null) {
         _log.log(VhmLevel.USER, "VHM: "+_clusterName+" - completed instruction to "+toString());
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, true, 100, 0, null, null);
         /* Note RouteKey is encaspulated in messageCallback */
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   @Override
   public boolean isExclusive() {
      return true;         /* We only want to know about the latest queued event */
   }

   /**
    * Describes the instruction
    */
   @Override
   public String toString() {
      StringBuilder buf = new StringBuilder();
      if (_action != null) {
         if (_action.equals(SerengetiLimitAction.actionSetTarget)) {
            return buf.append("set number of enabled compute nodes to ").append(_toSize).toString();
         } else if (_action.equals(SerengetiLimitAction.actionUnlimit)) {
            return buf.append("enable all compute nodes").toString();
         } else if (_action.equals(SerengetiLimitAction.actionWaitForManual)) {
            return buf.append("switch to manual mode").toString();
         }

         return _action.toString();
      }

      return "";
   }
}
