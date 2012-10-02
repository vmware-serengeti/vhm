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

   @Override
   public void sendMessage(byte[] data) {
      _connection.sendMessage(data);
   }
}
