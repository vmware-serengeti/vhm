/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

package com.vmware.vhadoop.vhm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import com.vmware.vhadoop.util.VhmLevel;
import com.vmware.vhadoop.vhm.hadoop.HadoopAdaptor;
import com.vmware.vhadoop.vhm.hadoop.SshUtilities.Credentials;
import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.vhm.rabbit.SimpleRabbitCredentials;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;
import com.vmware.vhadoop.vhm.strategy.BalancedVMChooser;
import com.vmware.vhadoop.vhm.strategy.JobTrackerEDPolicy;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;
import com.vmware.vhadoop.vhm.vc.VcAdapter;
import com.vmware.vhadoop.vhm.vc.VcCredentials;

public class BootstrapMain
{
   /* Class initialize early to avoid occasional NoClassDefFoundError in model testing
      Field is never used - public to avoid having it optimized out. Yes this is ugly. */
   public static VHMJsonReturnMessage _temp = VHMJsonReturnMessage.getVHMJsonReturnMessage();

   public static final String DEFAULT_VHM_CONFIG_FILENAME = "vhm.properties";
   public static final String DEFAULT_LOG_CONFIG_FILENAME = "logging.properties";
   public static final String DEFAULT_VHM_LOG_FILENAME = "vhm.xml";
   public static final String DEFAULT_VHM_HOME_DIR = "/tmp";
   public static final String DEFAULT_LOGS_SUBDIR = "/logs";
   public static final String DEFAULT_CONF_SUBDIR = "/conf";
   public static final String SERENGETI_HOME_DIR_PROP_KEY = "serengeti.home.dir";
   private static String CONFIG_DIR = DEFAULT_CONF_SUBDIR;

   private static Logger _log = Logger.getLogger(BootstrapMain.class.getName());

   private VCActions _vcActions;
   private HadoopActions _hadoopActions;
   private Properties _properties;

   public BootstrapMain() {
      this(DEFAULT_CONF_SUBDIR, buildVHMFilePath(DEFAULT_CONF_SUBDIR, DEFAULT_VHM_CONFIG_FILENAME), buildVHMFilePath(DEFAULT_LOGS_SUBDIR, DEFAULT_VHM_LOG_FILENAME));
   }

   public BootstrapMain(final String configDir, final String configFileName, final String logFileName) {
      if (configDir != null) {
         CONFIG_DIR = configDir;
      }

      String configFile = configFileName;
      if (configFile == null) {
         configFile = DEFAULT_VHM_CONFIG_FILENAME;
      }

      /* set a temporary log formatter so we get consistency */
      Logger.getLogger("").getHandlers()[0].setFormatter(new LogFormatter());

      _properties = readPropertiesFile(configFile);
      if (_properties == null) {
         _properties = new Properties();
      }

      String logFile = logFileName;
      if (logFile == null) {
         logFile = DEFAULT_VHM_LOG_FILENAME;
      }
      setupLogger(logFile);
   }

   private void setupLogger(final String fileName) {
      String loggingProperties = System.getProperty("java.util.logging.config.file");
      String loggingFlavour = "specified";
      if (loggingProperties == null) {
         loggingFlavour = "default";
         loggingProperties = buildVHMFilePath(CONFIG_DIR, DEFAULT_LOG_CONFIG_FILENAME);
      }

      InputStream is = null;
      try {
         /* read the properties file */
         Properties properties = readPropertiesFile(loggingProperties);
         /* ensure that the output file can be created */
         String key = "java.util.logging.FileHandler.pattern";
         if (properties.containsKey(key)) {
            File log = new File(properties.getProperty(key));
            File logDir = log.getParentFile();
            if (logDir != null && !logDir.canWrite()) {
               /* we need to alter where this goes */
               String file = buildVHMFilePath(DEFAULT_LOGS_SUBDIR, log.getName());
               _log.warning("The parent directory for log file specified by java.util.logging.FileHandler.pattern cannot be written to, falling back to "+file);
               properties.setProperty(key, file);
            }
         }

         /* construct an input stream from it */
         ByteArrayOutputStream output = new ByteArrayOutputStream();
         properties.store(output, null);
         is = new ByteArrayInputStream(output.toByteArray());

         LogManager.getLogManager().readConfiguration(is);
      } catch (Exception e) {
         _log.severe("The " + loggingFlavour + " logging properties file could not be read: " + loggingProperties);

         /* We've not got a properties file controlling things so use LogFormatter for the console at INFO level */
         LogFormatter formatter = new LogFormatter();
         Handler handlers[] = Logger.getLogger("").getHandlers();
         if (handlers.length == 0) {
            System.err.println("No log handlers defined, using default formatting");
         } else {
            handlers[0].setFormatter(formatter);
         }

         /* use default file name and LogFormatter for log file */
         try {
            FileHandler handler = new FileHandler(fileName);
            handler.setFormatter(formatter);
            Logger.getLogger("").addHandler(handler);
         } catch (SecurityException f) {
            f.printStackTrace();
         } catch (IOException f) {
            f.printStackTrace();
         }
      } finally {
         if (is != null) {
            try {
               is.close();
            } catch (IOException e) {
            }
         }
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
         File file;

         /* check for it in the conf directory if we've only got a base name, otherwise use the entire path
          * we have a specific check for '/' as it's a valid separator on windows, but is not the file.separator property.
          */
         if (!name.contains(System.getProperty("file.separator")) && !name.contains("/")) {
            file = new File(buildVHMFilePath(CONFIG_DIR, name));
            baseName = file.getName();
            if (file.canRead()) {
               is = new FileInputStream(file);
            }
         } else {
            file = new File(name);
            baseName = file.getName();

            if (file.canRead()) {
               is = new FileInputStream(file);
            }
         }

         /* check for it as a resource - this is always our base if it exists */
         resource = ClassLoader.getSystemResourceAsStream(baseName);
         if (resource != null) {
            _log.log(VhmLevel.USER, "VHM: loading default "+baseName+" from classloader resource");
            properties = new Properties();
            properties.load(resource);
         }

         /* if we've got version from the file system, overlay that on our resource based version if present */
         if (is != null) {
            if (properties != null) {
               _log.log(VhmLevel.USER, "VHM: overlaying "+baseName+" values from "+file.getPath());
            } else {
               _log.log(VhmLevel.USER, "VHM: loading "+baseName+" from "+file.getPath());
            }

            properties = new Properties(properties);
            properties.load(is);
         }
      } catch (IOException e) {
         _log.warning("Unable to read properties file from filesystem or as a resource from the jar files:" + name);
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
      if (vhm != null) {
         vhm.start();
      }
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

   HadoopActions getHadoopInterface(ThreadLocalCompoundStatus tlcs) {
      if (_hadoopActions == null) {
         _hadoopActions = new HadoopAdaptor(new Credentials(_properties.getProperty("vHadoopUser"),
                                                                        _properties.getProperty("vHadoopPwd"),
                                                                        _properties.getProperty("vHadoopPrvkeyFile")),
                                            new JTConfigInfo(_properties.getProperty("vHadoopHome"),
                                                             _properties.getProperty("vHadoopExcludeTTFile")),
                                            tlcs);
      }
      return _hadoopActions;
   }

   public Properties getProperties() {
      return _properties;
   }

   ScaleStrategy[] getScaleStrategies(final ThreadLocalCompoundStatus tlcs) {
      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(new BalancedVMChooser(), new JobTrackerEDPolicy(getHadoopInterface(tlcs), getVCInterface(tlcs)));
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
         public Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData clusterData, String clusterId, boolean isNewCluster, boolean isClusterViable) {
            return null;
         }
      };
   }

   VHM initVHM(final ThreadLocalCompoundStatus tlcs) {
      VHM vhm;

      MQClient mqClient = getRabbitInterface();

      vhm = new VHM(getVCInterface(tlcs), getScaleStrategies(tlcs), getStrategyMapper(), tlcs);
      ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(getVCInterface(tlcs), _properties.getProperty("uuid"));

      if (!vhm.registerEventProducer(cscl)) {
         _log.severe("Fatal error registering ClusterStateChangeListenerImpl as an event producer");
         return null;
      }
      if (!vhm.registerEventProducer(mqClient)) {
         _log.severe("Fatal error registering MQClient as an event producer");
         return null;
      }

      return vhm;
   }
}
