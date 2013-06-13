package com.vmware.vhadoop.vhm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.api.vhm.HadoopActions.JTConfigInfo;
import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
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

public class BootstrapMain
{
   public static final String DEFAULT_VHM_CONFIG_FILENAME = "vhm.properties";
   public static final String DEFAULT_LOG_CONFIG_FILENAME = "logging.properties";
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
      if (_properties == null) {
         _properties = new Properties();
      }
      setupLogger(logFileName);
   }

   private void setupLogger(final String fileName) {
      String loggingProperties = System.getProperty("java.util.logging.config.file");
      String loggingFlavour = "specified";
      if (loggingProperties == null) {
         loggingFlavour = "default";
         loggingProperties = buildVHMFilePath(DEFAULT_CONF_SUBDIR, DEFAULT_LOG_CONFIG_FILENAME);
      }

      InputStream is = null;
      try {
         is = new FileInputStream(loggingProperties);
         LogManager.getLogManager().readConfiguration(is);
      } catch (Exception e) {
         System.err.println("The " + loggingFlavour + " logging properties file could not be read: " + loggingProperties);
      } finally {
         if (is != null) {
            try {
               is.close();
            } catch (IOException e) {}
         }
      }

      Handler handlers[] = Logger.getLogger("").getHandlers();
      if (handlers.length == 0) {
         System.err.println("No log handlers defined, using default formatting");
      } else {
         handlers[0].setFormatter(new LogFormatter());
      }

      try {
         String name = fileName;
         if (name == null) {
            name = DEFAULT_VHM_LOG_FILENAME;
         }
         FileHandler handler = new FileHandler(name);
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

   /**
    * Try to locate this file in the expected location for configuration files if it's just the base name. If it's
    * a path, then use it directly. If the file can't be found, then see if there's a resource that corresponds to
    * the base name that supplies default values.
    *
    * @param name
    * @return
    */
   public static Properties readPropertiesFile(final String name) {
      Properties properties = null;
      InputStream is = null;
      InputStream resource = null;

      if (name == null) {
         return properties;
      }

      try {
         String baseName;

         /* check for it in the conf directory if we've only got a base name, otherwise use the entire path */
         if (!name.contains(System.getProperty("file.separator"))) {
            File file = new File(buildVHMFilePath(DEFAULT_CONF_SUBDIR, name));
            baseName = name;
            if (file.canRead()) {
               is = new FileInputStream(file);
            }
         } else {
            File file = new File(name);
            baseName = file.getName();

            if (file.canRead()) {
               is = new FileInputStream(file);
            }
         }

         /* check for it as a resource */
         resource = ClassLoader.getSystemResourceAsStream(baseName);
         if (resource != null) {
            properties = new Properties();
            properties.load(resource);
         }

         if (is != null) {
            properties = new Properties(properties);
            properties.load(is);
         }
      } catch (IOException e) {
         System.err.println("Unable to read properties file from filesystem or as a resource from the jar files:" + name);
      } finally {
         if (resource != null) {
            try {
               resource.close();
            } catch (IOException e) {}
         }
         if (is != null) {
            try {
               is.close();
            } catch (IOException e) {}
         }
      }

      return properties;
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

         _vcActions = new VcAdapter(vcCreds, _properties.getProperty("uuid"));
         ((VcAdapter) _vcActions).setThreadLocalCompoundStatus(tlcs);
      }
      return _vcActions;
   }


   MQClient getRabbitInterface() {
      return new RabbitAdaptor(new SimpleRabbitCredentials(_properties.getProperty("msgHostName"), _properties.getProperty("exchangeName"),
            _properties.getProperty("routeKeyCommand"), _properties.getProperty("routeKeyStatus")));
   }

   HadoopActions getHadoopInterface() {
      if (_hadoopActions == null) {
         _hadoopActions = new HadoopAdaptor(new SimpleHadoopCredentials(_properties.getProperty("vHadoopUser"), _properties.getProperty("vHadoopPwd"),
               _properties.getProperty("vHadoopPrvkeyFile")), new JTConfigInfo(_properties.getProperty("vHadoopHome"), _properties.getProperty("vHadoopExcludeTTFile")));
      }
      return _hadoopActions;
   }

   public Properties getProperties() {
      return _properties;
   }

   ScaleStrategy[] getScaleStrategies(final ThreadLocalCompoundStatus tlcs) {
      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(new BalancedVMChooser(), new JobTrackerEDPolicy(getHadoopInterface(), getVCInterface(tlcs)));
      return new ScaleStrategy[] { manualScaleStrategy };
   }

   ExtraInfoToClusterMapper getStrategyMapper() {
      return new ExtraInfoToClusterMapper() {
         @Override
         public String getStrategyKey(SerengetiClusterVariableData clusterData, String clusterId) {
            return ManualScaleStrategy.MANUAL_SCALE_STRATEGY_KEY;
         }

         @Override
         public Map<String, String> parseExtraInfo(SerengetiClusterVariableData clusterData, String clusterId) {
            return null;
         }

         @Override
         public Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData clusterData, String clusterId, boolean isNewCluster) {
            return null;
         }
      };
   }

   VHM initVHM(final ThreadLocalCompoundStatus tlcs) {
      VHM vhm;

      MQClient mqClient = getRabbitInterface();

      vhm = new VHM(getVCInterface(tlcs), getScaleStrategies(tlcs), getStrategyMapper(), tlcs);
      ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(getVCInterface(tlcs), _properties.getProperty("uuid"));

      vhm.registerEventProducer(cscl);
      vhm.registerEventProducer(mqClient);

      return vhm;
   }
}
