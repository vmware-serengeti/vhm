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
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;

public class SerengetiLimitInstruction extends AbstractClusterScaleEvent {
   public static final String actionSetTarget = "SetTarget";
   public static final String actionUnlimit = "Unlimit";
   public static final String actionWaitForManual = "WaitForManual";

   private final String _action;
   private final String _clusterFolderName;
   private final int _toSize;
   private final QueueClient _messageCallback;

   private static final Logger _log = Logger.getLogger(SerengetiLimitInstruction.class.getName());

   public SerengetiLimitInstruction(String clusterFolderName, String action, int toSize, QueueClient messageCallback) {
      _action = action;
      _clusterFolderName = clusterFolderName;
      _toSize = toSize;
      _messageCallback = messageCallback;
   }

   public String getAction() {
      return _action;
   }

   public String getClusterFolderName() {
      return _clusterFolderName;
   }

   public int getToSize() {
      return _toSize;
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
         _log.info("Reporting error "+message);
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, false, 100, 0, message, null);
         /* Note RouteKey is encaspulated in messageCallback */
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   public void reportCompletion() {
      if (_messageCallback != null) {
         _log.info("Reporting completion");
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, true, 100, 0, null, null);
         /* Note RouteKey is encaspulated in messageCallback */
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   @Override
   public boolean isExclusive() {
      return true;         /* We only want to know about the latest queued event */
   }
}
