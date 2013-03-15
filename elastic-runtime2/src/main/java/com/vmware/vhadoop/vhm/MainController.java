package com.vmware.vhadoop.vhm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.vmware.vhadoop.vhm.vc.VcVlsi;
import com.vmware.vhadoop.vhm.vc.VcCredentials;
import com.vmware.vim.vmomi.client.Client;

public class MainController {
   static Properties properties = null;
   static String vhmConfigFileName = "vhm.properties";
   
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
  
   public static void setup() {
      String configFileName = getVHMFileName("conf", vhmConfigFileName);
      readPropertiesFile(configFileName);
      System.out.println(Thread.currentThread().getName()+": vcid=" +  properties.getProperty("vCenterId"));
      com.vmware.vhadoop.vhm.vc.VcCredentials vcCreds = new VcCredentials();
      vcCreds.vcIP = properties.getProperty("vCenterId");
      vcCreds.vcThumbprint = properties.getProperty("vCenterThumbprint");
      
      vcCreds.user = properties.getProperty("vCenterUser");
      vcCreds.password = properties.getProperty("vCenterPwd");
      
      vcCreds.keyStoreFile = properties.getProperty("keyStorePath");
      vcCreds.keyStorePwd = properties.getProperty("keyStorePwd");
      vcCreds.vcExtKey = properties.getProperty("extensionKey");
      
      VcVlsi vcVlsi = new VcVlsi();
      try {
         Client vcClient = vcVlsi.connect(vcCreds, true, false);
         
         Client cloneClient = vcVlsi.connect(vcCreds, true, true);

         vcVlsi.testPC(cloneClient, properties.getProperty("uuid"));

      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
   }
   
   public static void main(String[] args) {
      setup();
      
   }

}
