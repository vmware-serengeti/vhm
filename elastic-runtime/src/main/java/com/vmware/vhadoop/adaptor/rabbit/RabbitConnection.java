package com.vmware.vhadoop.adaptor.rabbit;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Encapsulates the details of connecting to a RabbitMQ queue
 * 
 * @author bcorrie
 *
 */
public class RabbitConnection {
   
   interface RabbitCredentials {
      public String getHostName();
      public String getExchangeName();
      public String getRouteKeyCommand();
      public String getRouteKeyStatus();
   }

   private ConnectionFactory _connectionFactory;
   private RabbitCredentials _credentials;
   private Channel _channel;

   public RabbitConnection(RabbitCredentials credentials) {
      _credentials = credentials;
      _connectionFactory = new ConnectionFactory();
      _connectionFactory.setHost(credentials.getHostName());
   }
   
   /* TODO: Can we cache the consumer object? */
   QueueingConsumer getConsumer() {
      try {
         Connection connection = _connectionFactory.newConnection();
         _channel = connection.createChannel();

         String exchangeName = _credentials.getExchangeName();
         _channel.exchangeDeclare(exchangeName, "direct", true, false, null); /* TODO: Externalize? */
         String queueName = _channel.queueDeclare().getQueue();
         _channel.queueBind(queueName, exchangeName, _credentials.getRouteKeyCommand());

         QueueingConsumer consumer = new QueueingConsumer(_channel);
         _channel.basicConsume(queueName, true, consumer);
         return consumer;

      } catch (Exception e) {
         throw new RuntimeException("Unable to get message consumer", e);
      }
   }
   
   void sendMessage(byte[] data) {
      try {
         _channel.basicPublish(_credentials.getExchangeName(), _credentials.getRouteKeyStatus(), null, data);
      } catch (IOException e) {
         throw new RuntimeException("Unable to send message", e);
      }
   }
}
