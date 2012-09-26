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
