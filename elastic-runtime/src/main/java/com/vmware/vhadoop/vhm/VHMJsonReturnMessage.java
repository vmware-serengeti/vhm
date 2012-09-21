package com.vmware.vhadoop.vhm;

import com.google.gson.Gson;
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMReturnMessage;

public class VHMJsonReturnMessage implements VHMReturnMessage {
  
   // Serengeti status update interface JSON packet definition  
   Boolean finished;
   Boolean succeed;
   int progress;
   int error_code;
   String error_msg;

   
   public VHMJsonReturnMessage(
          Boolean param_finished,
          Boolean param_succeed,
          int param_progress,
          int param_error_code,
          String param_error_msg) {
      finished = param_finished;
      succeed = param_succeed;
      progress = param_progress;
      error_code = param_error_code;
      error_msg = param_error_msg;
   }
   
   
   @Override
   public byte[] getRawPayload() {
      Gson gson = new Gson();
      return gson.toJson(this).getBytes();
   }

}
