package com.vmware.vhadoop.vhm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.vhm.vc.VcAdapter;
import com.vmware.vhadoop.vhm.vc.VcCredentials;

public class MainController {
   private static Properties properties = null;
   private static String vhmConfigFileName = "vhm.properties";
   
   public static String getVHMFileName(String subdir, String fileName) {
      String homeDir = System.getProperties().getProperty("serengeti.home.dir");
      StringBuilder builder = new StringBuilder();
      if (homeDir != null && homeDir.length() > 0) {
         builder.append(homeDir).append(File.separator).append(subdir).append(File.separator).append(fileName);
      } else {
         builder.append("/tmp").append(File.separator).append(fileName);
      }
      String configFileName = builder.toString();
      return configFileName;
   }

   public static boolean readPropertiesFile(String fileName) {
      try {
          File file = new File(fileName);
          FileInputStream fileInput = new FileInputStream(file);
          properties = new Properties();
          properties.load(fileInput);
          fileInput.close();
          return true;
      } catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
          e.printStackTrace();
      }
      return false;
  }
  
   public static VCActions setupVC() {
      VcCredentials vcCreds = new VcCredentials();
      vcCreds.vcIP = properties.getProperty("vCenterId");
      vcCreds.vcThumbprint = properties.getProperty("vCenterThumbprint");
      
      vcCreds.user = properties.getProperty("vCenterUser");
      vcCreds.password = properties.getProperty("vCenterPwd");
      
      vcCreds.keyStoreFile = properties.getProperty("keyStorePath");
      vcCreds.keyStorePwd = properties.getProperty("keyStorePwd");
      vcCreds.vcExtKey = properties.getProperty("extensionKey");
      
      return new VcAdapter(vcCreds);
   }

   public static void readConfigFile() {
      String configFileName = getVHMFileName("conf", vhmConfigFileName);
      readPropertiesFile(configFileName);


   }
   
   public static Properties getProperties() {
      return properties;
   }
   
   public static void main(String[] args) {
      readConfigFile();
      VCActions vcActions = setupVC();
      ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(vcActions, 
            properties.getProperty("uuid"));
      
   }

}
