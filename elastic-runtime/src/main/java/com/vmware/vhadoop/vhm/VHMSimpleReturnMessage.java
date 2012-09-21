package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMReturnMessage;

public class VHMSimpleReturnMessage implements VHMReturnMessage {
   boolean _success;
   
   public VHMSimpleReturnMessage(boolean completedSuccess, String msg) {
      _success = completedSuccess;
   }
   
   /* TODO: Implement some sort of return message format */
   @Override
   public byte[] getRawPayload() {
      if (true == _success) {
         return "success".getBytes();
      } else {
         return "failure".getBytes();
      }
   }

}
