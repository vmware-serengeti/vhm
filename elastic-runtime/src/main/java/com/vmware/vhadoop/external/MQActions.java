package com.vmware.vhadoop.external;

public interface MQActions {

   public interface MessagePayload {
      public byte[] getRawPayload();
   }
   
   public MessagePayload blockAndReceive();
   
   public void interrupt();

}
