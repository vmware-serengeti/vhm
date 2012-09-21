package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.external.MQActions;
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMReturnMessage;

public class VHMProgressUpdater {
   private MQActions _mq;

   public VHMProgressUpdater(MQActions mq) {
      _mq = mq;
   }

   private void sendMessage(VHMJsonReturnMessage msg) {
      if (_mq != null) {
         _mq.sendMessage(msg.getRawPayload());
      }
   }
   
   public void setPercentDone(int progress) {
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(false, false, progress, 0, null);
      sendMessage(msg);
   }

   public void succeeded() {
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, true, 100, 0, null);
      sendMessage(msg);
   }

   public void error(String errorMsg) {
      VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, false, 100, 0, errorMsg);
      sendMessage(msg);
   }
}
