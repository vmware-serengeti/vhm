package com.vmware.vhadoop.api.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public interface EventProducer extends ClusterMapReader {

   public void registerEventConsumer(EventConsumer vhm);

   public void start();

   
}
