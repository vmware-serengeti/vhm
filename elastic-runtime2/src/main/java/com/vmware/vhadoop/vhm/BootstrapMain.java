package com.vmware.vhadoop.vhm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeListener;
import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.api.vhm.HadoopActions.JTConfigInfo;
import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToScaleStrategyMapper;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.vhm.hadoop.HadoopAdaptor;
import com.vmware.vhadoop.vhm.hadoop.SimpleHadoopCredentials;
import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.vhm.rabbit.SimpleRabbitCredentials;
import com.vmware.vhadoop.vhm.strategy.BalancedVMChooser;
import com.vmware.vhadoop.vhm.strategy.JobTrackerEDPolicy;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;
import com.vmware.vhadoop.vhm.vc.VcAdapter;
import com.vmware.vhadoop.vhm.vc.VcCredentials;

public class BootstrapMain {
   public static final String DEFAULT_VHM_CONFIG_FILENAME = "vhm.properties";
   public static final String DEFAULT_VHM_LOG_FILENAME = "vhm.xml";
   public static final String DEFAULT_VHM_HOME_DIR = "/tmp";
   public static final String DEFAULT_LOGS_SUBDIR = "/logs";
   public static final String DEFAULT_CONF_SUBDIR = "/conf";
   public static final String SERENGETI_HOME_DIR_PROP_KEY = "serengeti.home.dir";

   private VCActions _vcActions;
   private HadoopActions _hadoopActions;
   private Properties _properties;
   
   public BootstrapMain() {
      this(DEFAULT_VHM_CONFIG_FILENAME, DEFAULT_VHM_LOG_FILENAME);
   }
   
   public BootstrapMain(String configFileName, String logFileName) {
      _properties = readPropertiesFile(buildVHMFilePath(DEFAULT_CONF_SUBDIR, configFileName));
      setupLogger(buildVHMFilePath(DEFAULT_LOGS_SUBDIR, logFileName));
   }

   private void setupLogger(String fileName) {
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
  
   private String buildVHMFilePath(String subdir, String fileName) {
      String homeDir = System.getProperties().getProperty(SERENGETI_HOME_DIR_PROP_KEY);
      StringBuilder builder = new StringBuilder();
      if (homeDir != null && homeDir.length() > 0) {
         builder.append(homeDir).append(File.separator).append(subdir).append(File.separator).append(fileName);
      } else {
         builder.append(DEFAULT_VHM_HOME_DIR).append(File.separator).append(fileName);
      }
      return builder.toString();
   }

   private Properties readPropertiesFile(String fileName) {
      try {
          File file = new File(fileName);
          FileInputStream fileInput = new FileInputStream(file);
          Properties properties = new Properties();
          properties.load(fileInput);
          fileInput.close();
          return properties;
      } catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
          e.printStackTrace();
      }
      return null;
   }

   public static void main(String[] args) {
      BootstrapMain bm = new BootstrapMain();
      VHM vhm = bm.initVHM();
      vhm.start();
   }

   public VCActions getVCInterface() {
      if (_vcActions == null) {
         VcCredentials vcCreds = new VcCredentials();
         vcCreds.vcIP = _properties.getProperty("vCenterId");
         vcCreds.vcThumbprint = _properties.getProperty("vCenterThumbprint");
         
         vcCreds.user = _properties.getProperty("vCenterUser");
         vcCreds.password = _properties.getProperty("vCenterPwd");
         
         vcCreds.keyStoreFile = _properties.getProperty("keyStorePath");
         vcCreds.keyStorePwd = _properties.getProperty("keyStorePwd");
         vcCreds.vcExtKey = _properties.getProperty("extensionKey");
         
         return new VcAdapter(vcCreds);
      }
      return _vcActions;
   }

   private HadoopActions getHadoopInterface() {
      if (_hadoopActions == null) {
         _hadoopActions = new HadoopAdaptor(
               new SimpleHadoopCredentials(
                     _properties.getProperty("vHadoopUser"), 
                     _properties.getProperty("vHadoopPwd"),
                     _properties.getProperty("vHadoopPrvkeyFile")),
                     new JTConfigInfo(
                           _properties.getProperty("vHadoopHome"),
                           _properties.getProperty("vHadoopExcludeTTFile")));      
      }
      return _hadoopActions;
   }

   public Properties getProperties() {
      return _properties;
   }

   ScaleStrategy[] getScaleStrategies() {
      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(
            new BalancedVMChooser(), new JobTrackerEDPolicy(getHadoopInterface(), getVCInterface()));
      return new ScaleStrategy[]{manualScaleStrategy};
   }
   
   ExtraInfoToScaleStrategyMapper getStrategyMapper() {
      return new ExtraInfoToScaleStrategyMapper() {
         @Override
         public String getStrategyKey(VMEventData vmd) {
            return ManualScaleStrategy.MANUAL_SCALE_STRATEGY_KEY;
         }
      };
   }
   
   VHM initVHM() {
      VHM vhm;
            
      MQClient mqClient = new RabbitAdaptor(new SimpleRabbitCredentials(_properties.getProperty("msgHostName"),
            _properties.getProperty("exchangeName"),
            _properties.getProperty("routeKeyCommand"),
            _properties.getProperty("routeKeyStatus")));

      vhm = new VHM(getVCInterface(), getScaleStrategies(), getStrategyMapper());
      ClusterStateChangeListener cscl = new ClusterStateChangeListenerImpl(getVCInterface(), _properties.getProperty("uuid"));
      
      vhm.registerEventProducer(cscl);
      vhm.registerEventProducer(mqClient);
      
      return vhm;
   }
}
