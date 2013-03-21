package com.vmware.vhadoop.vhm;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.VCActions;

public class ClusterMapTest {
   static VHM _vhm;
   static ClusterStateChangeListenerImpl _cscl;

   @Test
   public void test() {
      MainController.readConfigFile();
      VCActions vcActions = MainController.setupVC(); 
      Properties properties = MainController.getProperties();
      
      _vhm = new VHM(vcActions);
      _cscl = new ClusterStateChangeListenerImpl(_vhm.getVCActions(), properties.getProperty("uuid"));
      
      _vhm.registerEventProducer(_cscl);
            
      _vhm.start();
      while (true) {
         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
   }

}
