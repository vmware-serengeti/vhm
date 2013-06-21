package com.vmware.vhadoop.api.vhm;

/* Represents actions which can be invoked on the Rabbit MQ subsystem */
public interface QueueClient
{
   void sendMessage(byte[] data);
}
