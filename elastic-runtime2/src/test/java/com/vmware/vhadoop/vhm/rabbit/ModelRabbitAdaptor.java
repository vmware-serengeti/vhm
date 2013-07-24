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
