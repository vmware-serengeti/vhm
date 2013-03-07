package com.vmware.vhadoop.api.vhm.events;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public interface EventProducer extends ClusterMapReader {

   public void registerConsumer(EventConsumer vhm);

   public void start();

   
}
