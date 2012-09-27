package com.vmware.vhadoop.vhm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.Level;

import sun.rmi.runtime.Log;

import com.vmware.vhadoop.adaptor.hadoop.HadoopAdaptor;
import com.vmware.vhadoop.adaptor.hadoop.HadoopConnection;
import com.vmware.vhadoop.adaptor.hadoop.JTConfig;
import com.vmware.vhadoop.adaptor.hadoop.SimpleHadoopCredentials;
import com.vmware.vhadoop.adaptor.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.adaptor.rabbit.SimpleRabbitCredentials;
import com.vmware.vhadoop.adaptor.vc.SimpleVCCredentials;
import com.vmware.vhadoop.adaptor.vc.VCAdaptor;
import com.vmware.vhadoop.adaptor.vc.VCUtils;
import com.vmware.vhadoop.external.HadoopActions;
import com.vmware.vhadoop.external.MQActions;
import com.vmware.vhadoop.external.VCActions;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.vhm.edpolicy.EDP_DeRecommissionTTs;
import com.vmware.vhadoop.vhm.edpolicy.EDP_JustPowerTTOnOff;
import com.vmware.vhadoop.vhm.edpolicy.EnableDisableTTPolicy;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMCA_BalancedVMChooser;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMCA_DumbVMChooser;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMChooserAlgorithm;

public class MainController {
   static Properties properties = null;
   static String vhmConfigFileName = "vhmProperties";
   static String legacyVhmConfigFileName = "vHadoopProperties";
   static String vhmLogFileName = "vhm.xml";
   
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
   
   public static void setupLogger(String fileName) {
       Logger.getLogger("").getHandlers()[0].setFormatter(new LogFormatter());
	   try {
           FileHandler handler = new FileHandler(fileName);
           Logger.getLogger("").addHandler(handler);
	   } catch (SecurityException e) {
		   e.printStackTrace();
	   } catch (IOException e) {
		   e.printStackTrace();
	   }
   }
   
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

   public static void main(String[] args) {
	   String logFileName = getVHMFileName("logs", vhmLogFileName);
	   setupLogger(logFileName);
	   String configFileName = getVHMFileName("conf", vhmConfigFileName);
       if (!readPropertiesFile(configFileName)) {
    	   configFileName = getVHMFileName("conf", legacyVhmConfigFileName);
    	   readPropertiesFile(configFileName);
       }
       
       /* TODO: As we build these subsystems, we should be checking that they're operational
        * and putting in decent error handling if they're not */
       
       VCUtils.trustAllHttpsCertificates();         /* TODO: BAD??? */
       VCActions vc = new VCAdaptor(new SimpleVCCredentials(
             properties.getProperty("vCenterId"), 
             properties.getProperty("vCenterUser"),
             properties.getProperty("vCenterPwd")));
       
       MQActions mq = new RabbitAdaptor(
             new SimpleRabbitCredentials(
                   properties.getProperty("msgHostName"),
                   properties.getProperty("exchangeName"),
                   properties.getProperty("routeKeyCommand"),
                   properties.getProperty("routeKeyStatus")));

       HadoopActions hd = new HadoopAdaptor(
             new SimpleHadoopCredentials(
                   properties.getProperty("vHadoopUser"), 
                   properties.getProperty("vHadoopPwd")), 
             new JTConfig(
                   properties.getProperty("vHadoopHome"),
                   properties.getProperty("vHadoopExcludeTTFile")));
       
       Logger logger = Logger.getLogger(HadoopConnection.class.getName());
     
       VMChooserAlgorithm vmChooser = new VMCA_BalancedVMChooser();
       // VMChooserAlgorithm vmChooser = new VMCA_DumbVMChooser();
       EnableDisableTTPolicy enableDisablePolicy = new EDP_DeRecommissionTTs(vc, hd);
       //EnableDisableTTPolicy enableDisablePolicy = new EDP_JustPowerTTOnOff(vc);
       VHMConfig vhmc = new VHMConfig(vmChooser, enableDisablePolicy);

       EmbeddedVHM vhm = new EmbeddedVHM();
       vhm.init(vhmc, vc, mq, hd);
       vhm.start();
       /* TODO: Since start is blocking in an infinite loop, no point having any code here
        * Consider having the ability to cleanly stop or interrupt vhm in a separate thread
        * There is already vhm.stop() for that purpose */
   }
}
