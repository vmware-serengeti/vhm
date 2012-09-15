package com.vmware.vhadoop.adaptor.rabbit;

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
   }

   private ConnectionFactory _connectionFactory;
   private RabbitCredentials _credentials;

   public RabbitConnection(RabbitCredentials credentials) {
      _credentials = credentials;
      _connectionFactory = new ConnectionFactory();
      _connectionFactory.setHost(credentials.getHostName());
   }
   
   /* TODO: Can we cache the consumer object? */
   QueueingConsumer getConsumer() {
      try {
         Connection connection = _connectionFactory.newConnection();
         Channel channel = connection.createChannel();

         String exchangeName = _credentials.getExchangeName();
         channel.exchangeDeclare(exchangeName, "direct", true, false, null); /* TODO: Externalize? */
         String queueName = channel.queueDeclare().getQueue();
         channel.queueBind(queueName, exchangeName, _credentials.getRouteKeyCommand());

         QueueingConsumer consumer = new QueueingConsumer(channel);
         channel.basicConsume(queueName, true, consumer);
         return consumer;

      } catch (Exception e) {
         throw new RuntimeException("Unable to get message consumer", e);
      }
   }
}
