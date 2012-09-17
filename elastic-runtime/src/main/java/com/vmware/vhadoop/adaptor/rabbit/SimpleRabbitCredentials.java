package com.vmware.vhadoop.adaptor.rabbit;

import com.vmware.vhadoop.adaptor.rabbit.RabbitConnection.RabbitCredentials;

public class SimpleRabbitCredentials implements RabbitCredentials {

   private String _hostName;
   private String _exchangeName;
   private String _routeKeyCommand;
   private String _routeKeyStatus;

   public SimpleRabbitCredentials(String hostName,
         String exchangeName,
         String routeKeyCommand,
         String routeKeyStatus) {
      this._hostName = hostName;
      this._exchangeName = exchangeName;
      this._routeKeyCommand = routeKeyCommand; 
      this._routeKeyStatus = routeKeyStatus; 
   }
   
   @Override
   public String getHostName() {
      return _hostName;
   }

   @Override
   public String getExchangeName() {
      return _exchangeName;
   }

   @Override
   public String getRouteKeyCommand() {
      return _routeKeyCommand;
   }

   @Override
   public String getRouteKeyStatus() {
      return _routeKeyStatus;
   }

}
