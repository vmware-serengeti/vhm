package com.vmware.vhadoop.vhm;

import java.util.Properties;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.vhm.rabbit.SimpleRabbitCredentials;
import com.vmware.vhadoop.vhm.strategy.DumbEDPolicy;
import com.vmware.vhadoop.vhm.strategy.DumbVMChooser;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;

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
      
      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(new DumbVMChooser(), new DumbEDPolicy(vcActions));
      
      _vhm = new VHM(vcActions, new ScaleStrategy[]{manualScaleStrategy});
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
