package com.vmware.vhadoop.vhm;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.vhm.rabbit.SimpleRabbitCredentials;

public class ClusterMapTest {
   VHM _vhm;
   ClusterStateChangeListenerImpl _cscl;
   MQClient _mqClient;

   @Test
   public void test() {
      MainController.readConfigFile();
      VCActions vcActions = MainController.setupVC(); 
      Properties properties = MainController.getProperties();
      
      _mqClient = new RabbitAdaptor(new SimpleRabbitCredentials(properties.getProperty("msgHostName"),
            properties.getProperty("exchangeName"),
            properties.getProperty("routeKeyCommand"),
            properties.getProperty("routeKeyStatus")));
      
      _vhm = new VHM(vcActions);
      _cscl = new ClusterStateChangeListenerImpl(_vhm.getVCActions(), properties.getProperty("uuid"));
      
      _vhm.registerEventProducer(_cscl);
      _vhm.registerEventProducer(_mqClient);
            
      _vhm.start();

      try {
         Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

}
