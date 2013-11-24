/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.vmware.vhadoop.api.vhm.QueueClient.CannotConnectException;

/**
 * Encapsulates the details of connecting to a RabbitMQ queue
 *
 */
public class RabbitConnection {
   private static final Logger _log = Logger.getLogger(RabbitConnection.class.getName());

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
   private volatile boolean _started = false;
   
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
            _connection = null;
            _connection = _connectionFactory.newConnection();
            _connection.addShutdownListener(new ShutdownListener() {
               @Override
               public void shutdownCompleted(ShutdownSignalException cause) {
                  _log.info("Connection shut down");
                  _log.log(Level.FINE, "{0}", cause.getReason());
                  synchronized(_connectionLock) {
                     _connection = null;
                     _channel = null;
                     _queueName = null;
                  }
                  _started = false;
               }
            });
            _started = true;
            _log.fine("Created new connection");
         }

         return _connection;
      }
   }

   private Channel getChannel() throws IOException {
      synchronized(_channelLock) {
         if (_channel == null || !_channel.isOpen()) {
            _log.fine("Creating new channel");
            _channel = null;
            Connection connection = getConnection();
            _channel = connection.createChannel();
            _channel.addShutdownListener(new ShutdownListener() {
               @Override
               public void shutdownCompleted(ShutdownSignalException cause) {
                  _log.info("Channel shut down");
                  _log.log(Level.FINE, "{0}", cause.getReason());
                  synchronized(_channelLock) {
                     _channel = null;
                     _queueName = null;
                  }
                  _started = false;
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
         _log.fine("Created transient queue: "+_queueName);
      }

      return _queueName;
   }

   /* TODO: Can we cache the consumer object? */
   QueueingConsumer getConsumer() throws CannotConnectException {
      synchronized(_consumerLock) {
         if (_consumer == null) {
            _log.fine("Creating new consumer");
            try {
               Channel channel = getChannel();
               _consumer = new QueueingConsumer(channel) {
                  @Override
                  public void handleShutdownSignal(java.lang.String consumerTag, ShutdownSignalException sig) {
                     super.handleShutdownSignal(consumerTag, sig);
                     _log.info("Consumer received shutdown notification");
                     _log.log(Level.FINE, "{0}", sig.getReason());

                     synchronized(_consumerLock) {
                        _consumer = null;
                     }
                  }
               };
               channel.basicConsume(getQueueName(), true, _consumer);
            } catch (Exception e) {
               throw new CannotConnectException("Unable to get message consumer", e);
            }
         }

         return _consumer;
      }
   }

   boolean connect() {
      Connection connection = null;
      try {
         connection = getConnection();
      } catch (IOException e) {
         /* squash */
      }

      return connection != null;
   }

   protected void sendMessage(String routeKey, byte[] data) throws CannotConnectException {
      boolean retry = false;
      do {
         try {
            Channel channel = getChannel();
            if (channel != null) {
               channel.basicPublish(_credentials.getExchangeName(), routeKey, null, data);
            }
            retry = false;
         } catch (Exception e) {
            if (!retry) {
               connect();
               retry = true;
            } else { 
               throw new CannotConnectException("Unable to send message", e);
            }
         }
      } while (retry);
   }

   protected void sendMessage(byte[] data) throws CannotConnectException {
      sendMessage(_credentials.getRouteKeyStatus(), data);
   }

   public boolean isShutdown() {
      return (_started == false);
   }
}
