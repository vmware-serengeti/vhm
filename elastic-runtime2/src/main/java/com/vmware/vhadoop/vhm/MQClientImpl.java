package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.api.vhm.events.EventConsumer;
import com.vmware.vhadoop.vhm.events.SerengetiLimitEvent;

import java.util.*;

public class MQClientImpl extends AbstractClusterMapReader implements com.vmware.vhadoop.api.vhm.MQClient {

   EventConsumer _eventConsumer;
   Map<String, String> _clusterUUIDMap;
   
   void setTestClusterUUIDMap(Map<String, String> clusterUUIDMap) {
      _clusterUUIDMap = clusterUUIDMap;
   }
   
   public void doSerengetiLimitAction(String clusterName, int from, int to) {
      String masterUUID = getMasterUUIDForCluster(clusterName);
      _eventConsumer.placeEventOnQueue(new SerengetiLimitEvent(masterUUID, from, to));
   }

   private String getMasterUUIDForCluster(String clusterName) {
      return _clusterUUIDMap.get(clusterName);
   }

   @Override
   public void registerEventConsumer(EventConsumer consumer) {
      _eventConsumer = consumer;
   }

   @Override
   public void start() {

   }
}
