/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

package com.vmware.vhadoop.vhm.rabbit;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Encapsulates the details of connecting to a RabbitMQ queue
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

   private Channel _channel = null;
   private Object _channelLock = new Object();
   private Connection _connection = null;
   private Object _connectionLock = new Object();
   private String _queueName = null;
   private QueueingConsumer _consumer = null;
   private Object _consumerLock = new Object();

   /* For testing only */
   protected RabbitConnection() {
   }

   public RabbitConnection(RabbitCredentials credentials) {
      _credentials = credentials;
      _connectionFactory = new ConnectionFactory();
      _connectionFactory.setHost(credentials.getHostName());
   }

   private Connection getConnection() throws IOException {
      synchronized(_connectionLock) {
         if (_connection == null || !_connection.isOpen()) {
            _connection = _connectionFactory.newConnection();
            _connection.addShutdownListener(new ShutdownListener() {
               @Override
               public void shutdownCompleted(ShutdownSignalException cause) {
                  synchronized(_connectionLock) {
                     _connection = null;
                     _channel = null;
                     _queueName = null;
                  }
               }
            });
         }

         return _connection;
      }
   }

   private Channel getChannel() throws IOException {
      synchronized(_channelLock) {
         if (_channel == null || !_channel.isOpen()) {
            Connection connection = getConnection();
            _channel = connection.createChannel();
            _channel.addShutdownListener(new ShutdownListener() {
               @Override
               public void shutdownCompleted(ShutdownSignalException cause) {
                  synchronized(_channelLock) {
                     _channel = null;
                     _queueName = null;
                  }
               }
            });
         }

         return _channel;
      }
   }

   private synchronized String getQueueName() throws IOException {
      if (_queueName == null) {
         String exchangeName = _credentials.getExchangeName();
         Channel channel = getChannel();
         channel.exchangeDeclare(exchangeName, "direct", true, false, null); /* TODO: Externalize? */
         _queueName = channel.queueDeclare().getQueue();
         channel.queueBind(_queueName, exchangeName, _credentials.getRouteKeyCommand());
      }

      return _queueName;
   }

   /* TODO: Can we cache the consumer object? */
   QueueingConsumer getConsumer() {
      synchronized(_consumerLock) {
         if (_consumer == null) {
            try {
               Channel channel = getChannel();
               _consumer = new QueueingConsumer(channel) {
                  @Override
                  public void handleShutdownSignal(java.lang.String consumerTag, ShutdownSignalException sig) {
                     synchronized(_consumerLock) {
                        _consumer = null;
                     }
                  }
               };
               channel.basicConsume(getQueueName(), true, _consumer);
            } catch (Exception e) {
               throw new RuntimeException("Unable to get message consumer", e);
            }
         }

         return _consumer;
      }
   }

   protected void sendMessage(String routeKey, byte[] data) {
      try {
         Channel channel = getChannel();
         if (channel != null) {
            channel.basicPublish(_credentials.getExchangeName(), routeKey, null, data);
         }
      } catch (IOException e) {
         throw new RuntimeException("Unable to send message", e);
      }
   }

   protected void sendMessage(byte[] data) {
      sendMessage(_credentials.getRouteKeyStatus(), data);
   }
}
