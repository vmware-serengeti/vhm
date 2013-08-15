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

package com.vmware.vhadoop.vhm.rabbit;

import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.QueueClient;
import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

/**
 * Message queue client implementation for RabbitMQ
 *
 */
public class ModelRabbitAdaptor implements MQClient {
   private static final Logger _log = Logger.getLogger(ModelRabbitAdaptor.class.getName());

   VirtualCenter vCenter;
   Serengeti serengeti;
   EventConsumer eventConsumer;
   boolean running;

   public static class RabbitConnectionCallback implements QueueClient {
      private String routeKey;
      private Serengeti serengeti;

      public RabbitConnectionCallback(String routeKey, Serengeti serengeti) {
         this.routeKey = routeKey;
         this.serengeti = serengeti;
      }

      @Override
      public void sendMessage(byte[] data) {
         if (routeKey == null) {
            serengeti.deliverMessage(data);
         } else {
            serengeti.deliverMessage(routeKey, data);
         }
      }
   }

   public ModelRabbitAdaptor(VirtualCenter vCenter) {
      this.vCenter = vCenter;
   }

   @Override
   public void sendMessage(byte[] data) {
      serengeti.deliverMessage(data);
   }

   @Override
   public void registerEventConsumer(EventConsumer eventConsumer) {
      this.eventConsumer = eventConsumer;
   }

   @Override
   public void start(final EventProducerStartStopCallback stoppingCallback) {
      /* no-op */
      running = true;
   }

   @Override
   public void stop() {
      /* no-op */
   }

   @Override
   public boolean isStopped() {
      return running;
   }
}
