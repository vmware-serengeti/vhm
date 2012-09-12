package com.vmware.vhadoop.adaptor.rabbit;

import com.vmware.vhadoop.adaptor.rabbit.RabbitConnection.RabbitCredentials;

public class SimpleRabbitCredentials implements RabbitCredentials {

   private String _hostName;
   private String _exchangeName;

   public SimpleRabbitCredentials(String hostName, String exchangeName) {
      this._hostName = hostName;
      this._exchangeName = exchangeName;
   }
   
   @Override
   public String getHostName() {
      return _hostName;
   }

   @Override
   public String getExchangeName() {
      return _exchangeName;
   }

}
