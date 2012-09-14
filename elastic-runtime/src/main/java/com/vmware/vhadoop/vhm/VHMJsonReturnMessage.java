package com.vmware.vhadoop.vhm;

import com.google.gson.Gson;
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMReturnMessage;

public class VHMJsonReturnMessage implements VHMReturnMessage {
  
   // TODO:  move to separate file?
   private class VHMStatusMessage {
      Boolean finished;
      Boolean succeed;
      int progress;
      int error_code;
      String error_msg;
   }

   private VHMStatusMessage _msg;
   
   public VHMJsonReturnMessage(boolean completedSuccess) {
      _msg = new VHMStatusMessage();
      _msg.finished = true;
      _msg.succeed = completedSuccess;
      _msg.progress = 100;
      _msg.error_code = 0;
      _msg.error_msg = null;
   }
   
   
   @Override
   public byte[] getRawPayload() {
      Gson gson = new Gson();
      return gson.toJson(_msg).getBytes();
   }

}
