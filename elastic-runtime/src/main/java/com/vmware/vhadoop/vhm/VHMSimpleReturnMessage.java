package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMReturnMessage;

public class VHMSimpleReturnMessage implements VHMReturnMessage {
   String _numTTs;
   
   public VHMSimpleReturnMessage(String numTTs) {
      _numTTs = numTTs;
   }
   
   /* TODO: Implement some sort of return message format */
   @Override
   public byte[] getRawPayload() {
      return _numTTs.getBytes();
   }

}
