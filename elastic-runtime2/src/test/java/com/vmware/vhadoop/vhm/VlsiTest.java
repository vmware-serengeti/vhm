package com.vmware.vhadoop.vhm;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.VCActions;


public class VlsiTest {

   @Test
   public void test() {
      MainController.readConfigFile();
      VCActions vcActions = MainController.setupVC(); 
      Properties properties = MainController.getProperties();
      vcActions.waitForPropertyChange(properties.getProperty("uuid"));
   }

}
