package com.vmware.vhadoop.api.vhm;

import com.vmware.vhadoop.api.vhm.events.EventProducer;

/**
 * Represents actions which can be invoked on the Rabbit MQ subsystem, grouped with the  EventProducer callback mechanism
 */
public interface MQClient extends EventProducer, QueueClient
{
}
