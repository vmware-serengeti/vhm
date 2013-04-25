package com.vmware.vhadoop.vhm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToScaleStrategyMapper;
import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.api.vhm.HadoopActions.JTConfigInfo;
import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
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
      this(buildVHMFilePath(DEFAULT_CONF_SUBDIR, DEFAULT_VHM_CONFIG_FILENAME), buildVHMFilePath(DEFAULT_LOGS_SUBDIR, DEFAULT_VHM_LOG_FILENAME));
   }

   public BootstrapMain(final String configFileName, final String logFileName) {
      _properties = readPropertiesFile(configFileName);
      setupLogger(logFileName);
   }

   private void setupLogger(final String fileName) {
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

   private static String buildVHMFilePath(final String subdir, final String fileName) {
      String homeDir = System.getProperties().getProperty(SERENGETI_HOME_DIR_PROP_KEY);
      StringBuilder builder = new StringBuilder();
      if (homeDir != null && homeDir.length() > 0) {
         builder.append(homeDir).append(File.separator).append(subdir).append(File.separator).append(fileName);
      } else {
         builder.append(DEFAULT_VHM_HOME_DIR).append(File.separator).append(fileName);
      }
      return builder.toString();
   }

   private Properties readPropertiesFile(final String fileName) {
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

   public static void main(final String[] args) {
      BootstrapMain bm = new BootstrapMain();
      ThreadLocalCompoundStatus tlcs = new ThreadLocalCompoundStatus();
      VHM vhm = bm.initVHM(tlcs);
      vhm.start();
   }

   public VCActions getVCInterface(final ThreadLocalCompoundStatus tlcs) {
      if (_vcActions == null) {
         VcCredentials vcCreds = new VcCredentials();
         vcCreds.vcIP = _properties.getProperty("vCenterId");
         vcCreds.vcThumbprint = _properties.getProperty("vCenterThumbprint");

         vcCreds.user = _properties.getProperty("vCenterUser");
         vcCreds.password = _properties.getProperty("vCenterPwd");

         vcCreds.keyStoreFile = _properties.getProperty("keyStorePath");
         vcCreds.keyStorePwd = _properties.getProperty("keyStorePwd");
         vcCreds.vcExtKey = _properties.getProperty("extensionKey");

         _vcActions = new VcAdapter(vcCreds);
         ((VcAdapter)_vcActions).setThreadLocalCompoundStatus(tlcs);
      }
      return _vcActions;
   }

   HadoopActions getHadoopInterface() {
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

   ScaleStrategy[] getScaleStrategies(final ThreadLocalCompoundStatus tlcs) {
      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(
            new BalancedVMChooser(), new JobTrackerEDPolicy(getHadoopInterface(), getVCInterface(tlcs)));
      return new ScaleStrategy[]{manualScaleStrategy};
   }

   ExtraInfoToScaleStrategyMapper getStrategyMapper() {
      return new ExtraInfoToScaleStrategyMapper() {
         @Override
         public String getStrategyKey(final VMEventData vmd) {
            return ManualScaleStrategy.MANUAL_SCALE_STRATEGY_KEY;
         }
      };
   }

   VHM initVHM(final ThreadLocalCompoundStatus tlcs) {
      VHM vhm;

      MQClient mqClient = new RabbitAdaptor(new SimpleRabbitCredentials(_properties.getProperty("msgHostName"),
            _properties.getProperty("exchangeName"),
            _properties.getProperty("routeKeyCommand"),
            _properties.getProperty("routeKeyStatus")));

      vhm = new VHM(getVCInterface(tlcs), getScaleStrategies(tlcs), getStrategyMapper(), tlcs);
      ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(getVCInterface(tlcs), _properties.getProperty("uuid"));

      vhm.registerEventProducer(cscl);
      vhm.registerEventProducer(mqClient);

      return vhm;
   }
}
