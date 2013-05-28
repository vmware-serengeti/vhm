package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.rabbit.RabbitConnection;

public class TestRabbitConnection extends RabbitConnection {

   private TestChannel _testChannel;

   public interface TestChannel {
      public void basicPublish(String routeKey, byte[] data);
   }

   public TestRabbitConnection(TestChannel testChannel) {
      _testChannel = testChannel;
   }
   
   @Override
   protected void sendMessage(String routeKey, byte[] data) {
      _testChannel.basicPublish(routeKey, data);
   }

   @Override
   protected void sendMessage(byte[] data) {
      _testChannel.basicPublish(null, data);
   }

}
