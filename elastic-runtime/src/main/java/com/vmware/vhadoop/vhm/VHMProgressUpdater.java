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

import com.vmware.vhadoop.external.MQActions;
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMReturnMessage;

public class VHMProgressUpdater {

   private static final Logger _log = Logger.getLogger(VHMProgressUpdater.class.getName());

   private MQActions _mq;
   private boolean _finished;

   public VHMProgressUpdater(MQActions mq) {
      _mq = mq;
      _finished = false;
   }

   private void sendMessage(VHMJsonReturnMessage msg) {
      if (_mq != null) {
         _mq.sendMessage(msg.getRawPayload());
      }
   }
   
   public void verifyCompletionStatus(boolean finished) {
      if (_finished != finished) {
         _log.log(Level.WARNING, "Expected " + finished);
      }
   }
   
   public void setPercentDone(int progress) {
      verifyCompletionStatus(false);
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(false, false, progress, 0, null);
      sendMessage(msg);
   }

   public void succeeded() {
      verifyCompletionStatus(false);
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, true, 100, 0, null);
      sendMessage(msg);
      _finished = true;
   }

   public void error(String errorMsg) {
      verifyCompletionStatus(false);
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, false, 100, 0, errorMsg);
      sendMessage(msg);
      _finished = true;
   }
}
