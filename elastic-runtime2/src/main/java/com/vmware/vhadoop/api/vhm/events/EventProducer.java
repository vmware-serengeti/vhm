package com.vmware.vhadoop.api.vhm.events;

/* High level interface that works with EventConsumer
 * Most of the main components that plug into VHM are EventProducers. The events produced then trigger responses in the VHM */
public interface EventProducer {

   public void registerEventConsumer(EventConsumer vhm);

   public void start();

   public void stop();
}
