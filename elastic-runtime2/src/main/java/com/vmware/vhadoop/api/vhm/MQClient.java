package com.vmware.vhadoop.api.vhm;

import com.vmware.vhadoop.api.vhm.events.EventProducer;

/* Represents actions which can be invoked on the Rabbit MQ subsystem */
public interface MQClient extends EventProducer {

   void sendMessage(byte[] data);

}
