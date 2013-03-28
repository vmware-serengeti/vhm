package com.vmware.vhadoop.api.vhm;

import com.vmware.vhadoop.api.vhm.events.EventProducer;

public interface MQClient extends EventProducer {

   void sendMessage(byte[] data);

}
