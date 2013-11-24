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
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.util.ExternalizedParameters;
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction;
import com.vmware.vhadoop.vhm.rabbit.RabbitConnection.RabbitCredentials;

/**
 * Message queue client implementation for RabbitMQ
 *
 */
public class RabbitAdaptor implements MQClient {
   private final RabbitConnection _connection;
   private EventConsumer _eventConsumer;
   private volatile boolean _started;
   private Thread _mainThread;

   long _startTime = System.currentTimeMillis();
   boolean _deliberateFailureTriggered = false;

   private static long CONNECTION_SHUTDOWN_TIMEOUT_MILLIS = ExternalizedParameters.get().getLong("RABBITMQ_CONNECTION_SHUTDOWN_TIMEOUT_MILLIS");

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
      public void sendMessage(byte[] data) throws CannotConnectException {
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
   public void sendMessage(byte[] data) throws CannotConnectException {
      _connection.sendMessage(data);
   }

   @Override
   public void registerEventConsumer(EventConsumer eventConsumer) {
      _eventConsumer = eventConsumer;
   }

   @Override
   public void start(final EventProducerStartStopCallback startStopCallback) {
      _started = true;
      _mainThread = new Thread(new Runnable() {
         @Override
         public void run() {
            _log.info("RabbitAdaptor starting...");
            startStopCallback.notifyStarted(RabbitAdaptor.this);
            while (_started) {
               /* wait for a rabbit instance to talk to */
               while (!_connection.connect()) {
                  try {
                     Thread.sleep(20000);
                  } catch (InterruptedException e) {
                     _log.warning("VHM: unexpected interruption while waiting for connection to RabbitMQ broker to complete");
                     if (!_started) {
                        /* We could have never properly initialized and are then being told to shut down - break the loop */
                        break;
                     }
                  }
               }

               if (_started) {
                  _log.info("Connected to Rabbit server");
               }

               try {
                  while (_started) {
                     _log.info("Rabbit queue waiting for message");
                     QueueingConsumer.Delivery delivery = _connection.getConsumer().nextDelivery();
                     _log.finest("Raw JSON: "+new String(delivery.getBody()));
                     VHMJsonInputMessage message = new VHMJsonInputMessage(delivery.getBody());
                     SerengetiLimitInstruction event = new SerengetiLimitInstruction(message.getClusterId(), message.getAction(), message.getInstanceNum(), new RabbitConnectionCallback(message.getRouteKey(), _connection));

                     /* log that we've received an explicit action from the serengeti client */
                     _log.log(VhmLevel.USER, "VHM: <%C"+message.getClusterId()+"%C> - instruction received from Serengeti client: "+event.toString());

                     _eventConsumer.placeEventOnQueue(event);

                     /* acknowledge receipt of the instruction immediately so that Serengeti isn't left hanging if the cluster scale thread is blocked by an in-flight operation */
                     event.acknowledgeReceipt();
                  }
               } catch (InterruptedException e) {
                  /* Almost certainly stop() was invoked */
               } catch (ShutdownSignalException e) {
                  _log.info("Rabbit queue shutting down");
                  /* This is not fatal, this is an expected expection when shutting down */
               } catch (Throwable t) {
                  _log.log(Level.SEVERE, "VHM: unexpected exception from RabbitMQ queue - "+ t.getMessage());
                  _log.log(Level.INFO, "VHM: unexpected exception from Rabbit queue ", t);
                  startStopCallback.notifyFailed(RabbitAdaptor.this);
               }
            }
            _log.info("RabbitAdaptor stopping...");
            waitForConnectionShutdown(CONNECTION_SHUTDOWN_TIMEOUT_MILLIS);
            startStopCallback.notifyStopped(RabbitAdaptor.this);
         }}, "MQClientImpl");
      _mainThread.start();
   }

   private void waitForConnectionShutdown(long timeoutMillis) {
      int timeoutCountdown = (int)timeoutMillis;
      final int sleepTimeMillis = 100;
      do {
         try {
            Thread.sleep(sleepTimeMillis);
         } catch (InterruptedException e) {}
      } while (!_connection.isShutdown() && ((timeoutCountdown -= sleepTimeMillis) > 0));
   }

   @Override
   public void stop() {
      _started = false;
      try {
         /* TODO: Is this the right approach? */
         _connection.getConsumer().getChannel().close();
      } catch (Exception e) {
         _log.log(Level.INFO, "Error shutting down MQClient "+e.getMessage());
         /* If we're in a situation where we fail to shut the queue down cleanly, ensure we interrupt the waiting thread */
         _mainThread.interrupt();
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
