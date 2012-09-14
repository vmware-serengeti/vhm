package com.vmware.vhadoop.adaptor.rabbit;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.rabbitmq.client.QueueingConsumer;
import com.vmware.vhadoop.adaptor.rabbit.RabbitConnection.RabbitCredentials;
import com.vmware.vhadoop.external.MQActions;
import com.vmware.vhadoop.vhm.VHMJsonInputMessage;

/**
 * Message queue client implementation for RabbitMQ
 * 
 * @author bcorrie
 *
 */
public class RabbitAdaptor implements MQActions {
   private RabbitConnection _connection;

   private static final Logger _log = Logger.getLogger(RabbitAdaptor.class.getName());

   public RabbitAdaptor(RabbitCredentials rabbitCredentials) {
      _connection = new RabbitConnection(rabbitCredentials);
   }

   @Override
   public MessagePayload blockAndReceive() {
      try {
         QueueingConsumer.Delivery delivery = _connection.getConsumer().nextDelivery();
         return new VHMJsonInputMessage(delivery.getBody());
      } catch (InterruptedException e) {
         _log.log(Level.INFO, "Message Queue interrupted");
      }
      return null;
   }
   
   @Override
   public void interrupt() {
      /* TODO: Figure out how to interrupt the consumer - wise to synchronize too
       * This is going to be important for clean shutdown */ 
   }

}
