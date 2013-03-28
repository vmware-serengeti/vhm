package com.vmware.vhadoop.api.vhm.events;

public interface EventProducer {

   public void registerEventConsumer(EventConsumer vhm);

   public void start();

   
}
