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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.QueueClient;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.rabbit.RabbitConnection.RabbitCredentials;

/**
 * Message queue client implementation for RabbitMQ
 *
 */
public class RabbitAdaptor implements MQClient {
   private RabbitConnection _connection;
   private EventConsumer _eventConsumer;
   private boolean _started;
   private Thread _mainThread;

   long _startTime = System.currentTimeMillis();
   boolean _deliberateFailureTriggered = false;

   private static final Logger _log = Logger.getLogger(RabbitAdaptor.class.getName());

   @SuppressWarnings("unused")
   private void deliberatelyFail(long afterTimeMillis) {
      if (!_deliberateFailureTriggered && (System.currentTimeMillis() > (_startTime + afterTimeMillis))) {
         _deliberateFailureTriggered = true;
         throw new RuntimeException("Deliberate failure!!");
      }
   }

   public static class RabbitConnectionCallback implements QueueClient {
      private String _routeKey;
      private RabbitConnection _innerConnection;

      public RabbitConnectionCallback(String routeKey, RabbitConnection connection) {
         _routeKey = routeKey;
         _innerConnection = connection;
      }

      @Override
      public void sendMessage(byte[] data) {
         if (_routeKey == null) {
            _innerConnection.sendMessage(data);
         } else {
            _innerConnection.sendMessage(_routeKey, data);
         }
      }
   }

   public RabbitAdaptor(RabbitCredentials rabbitCredentials) {
      _connection = new RabbitConnection(rabbitCredentials);
   }

   @Override
   public void sendMessage(byte[] data) {
      _connection.sendMessage(data);
   }

   @Override
   public void registerEventConsumer(EventConsumer eventConsumer) {
      _eventConsumer = eventConsumer;
   }

   @Override
   public void start(final EventProducerStoppingCallback stoppingCallback) {
      _started = true;
      _mainThread = new Thread(new Runnable() {
         @Override
         public void run() {
            boolean fatalError = false;
            while (_started) {
               /* wait for a rabbit instance to talk to */
               while (!_connection.connect()) {
                  try {
                     Thread.sleep(20000);
                  } catch (InterruptedException e) {
                     _log.warning("Unexpected interruption to sleep");
                  }
               }

               _log.info("Connected to Rabbit server");

               try {
                  while (_started) {
                     _log.info("Rabbit queue waiting for message");
                     QueueingConsumer.Delivery delivery = _connection.getConsumer().nextDelivery();
                     VHMJsonInputMessage message = new VHMJsonInputMessage(delivery.getBody());
                     ClusterScaleEvent event = new SerengetiLimitInstruction(message.getClusterId(),
                           message.getAction(),
                           message.getInstanceNum(), new RabbitConnectionCallback(message.getRouteKey(), _connection));

                     _eventConsumer.placeEventOnQueue(event);
                     _log.info("New Serengeti limit event placed on queue");
                  }
               } catch (InterruptedException e) {
                  /* Almost certainly stop() was invoked */
               } catch (ShutdownSignalException e) {
                  _log.info("Rabbit queue shutting down");
                  /* This is not fatal, this is an expected expection when shutting down */
               } catch (Throwable t) {
                  _log.log(Level.SEVERE, "Unexpected exception from Rabbit queue ", t);
                  fatalError = true;
               }
            }
            _log.info("RabbitAdaptor stopping...");
            if (stoppingCallback != null) {
               stoppingCallback.notifyStopping(RabbitAdaptor.this, fatalError);
            }
         }}, "MQClientImpl");
      _mainThread.start();
   }

   @Override
   public void stop() {
      _started = false;
      try {
         /* TODO: Is this the right approach? */
         _connection.getConsumer().getChannel().close();
      } catch (Exception e) {
         _log.log(Level.INFO, "Unexpected exception stopping MQClient", e);
      }
   }

   @Override
   public boolean isStopped() {
      if ((_mainThread == null) || (!_mainThread.isAlive())) {
         return true;
      }
      return false;
   }
}
