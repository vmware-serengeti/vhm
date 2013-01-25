/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
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
import com.vmware.vhadoop.adaptor.vc.SecureVCCredentials;
import com.vmware.vhadoop.adaptor.vc.SimpleVCCredentials;
import com.vmware.vhadoop.adaptor.vc.VCAdaptor;
import com.vmware.vhadoop.adaptor.vc.VCUtils;
import com.vmware.vhadoop.external.HadoopActions;
import com.vmware.vhadoop.external.MQActions;
import com.vmware.vhadoop.external.VCActions;
import com.vmware.vhadoop.util.LogFormatter;
import com.vmware.vhadoop.util.ProgressLogger;
import com.vmware.vhadoop.vhm.edpolicy.EDP_DeRecommissionTTs;
import com.vmware.vhadoop.vhm.edpolicy.EDP_JustPowerTTOnOff;
import com.vmware.vhadoop.vhm.edpolicy.EDP_SoftDeRecommissionTTs;
import com.vmware.vhadoop.vhm.edpolicy.EnableDisableTTPolicy;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMCA_BalancedVMChooser;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMCA_DumbVMChooser;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMChooserAlgorithm;

// Result class for MainController.setupActions()
class VHMActions {
   VCActions vc;
   MQActions mq;
   HadoopActions hd;
}

public class MainController {
   static Properties properties = null;
   static String vhmConfigFileName = "vhm.properties";
   static String legacyVhmConfigFileName = "vHadoopProperties";
   static String vhmLogFileName = "vhm.xml";
   
   private static final ProgressLogger _pLog = ProgressLogger.getProgressLogger(EmbeddedVHM.class.getName());
   private static final Logger _log = _pLog.getLogger();
   
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
   
   public static VCActions getVCAdaptor() {
      VCActions vc = null;
      
      // First try to get properties associated w/certificate VC login.
      try {
         VCUtils.trustAllHttpsCertificates(
              properties.getProperty("keyStorePath"),
              properties.getProperty("keyStorePwd")); 
         vc = new VCAdaptor(new SecureVCCredentials(
              properties.getProperty("vCenterId"), 
              properties.getProperty("extensionKey")));
      } catch (Exception e) {
         _log.log(Level.WARNING, "Falling back to user/password vc connection; Got exception initializing certificate vc connection properties: "+e);
      }
      if (vc != null) {
         return vc;
      }
      
      // If not available, next try to get properties associated w/user/password VC login.
      try {
         VCUtils.trustAllHttpsCertificates();
         vc = new VCAdaptor(new SimpleVCCredentials(
              properties.getProperty("vCenterId"),
              properties.getProperty("vCenterUser"),
              properties.getProperty("vCenterPwd")));
      } catch (Exception e) {
         _log.log(Level.WARNING, "Got exception initializing user/password vc connection properties: "+e);
      }
      
      return vc;
   }

   public static VHMActions setupActions() {
      VHMActions actions = new VHMActions();

      String configFileName = getVHMFileName("conf", vhmConfigFileName);
      if (!readPropertiesFile(configFileName)) {
         configFileName = getVHMFileName("conf", legacyVhmConfigFileName);
         readPropertiesFile(configFileName);
      }

      /* TODO: As we build these subsystems, we should be checking that they're operational
       * and putting in decent error handling if they're not */

      actions.vc = getVCAdaptor();

      actions.mq = new RabbitAdaptor(
            new SimpleRabbitCredentials(
                  properties.getProperty("msgHostName"),
                  properties.getProperty("exchangeName"),
                  properties.getProperty("routeKeyCommand"),
                  properties.getProperty("routeKeyStatus")));

      actions.hd = new HadoopAdaptor(
            new SimpleHadoopCredentials(
                  properties.getProperty("vHadoopUser"), 
                  properties.getProperty("vHadoopPwd"),
                  properties.getProperty("vHadoopPrvkeyFile")),
                  new JTConfig(
                        properties.getProperty("vHadoopHome"),
                        properties.getProperty("vHadoopExcludeTTFile")));

      return actions;
   }
   
   
   public static void main(String[] args) {
	   String logFileName = getVHMFileName("logs", vhmLogFileName);
	   setupLogger(logFileName);

      VHMActions actions = setupActions();
       
       VMChooserAlgorithm vmChooser = new VMCA_BalancedVMChooser();
       // VMChooserAlgorithm vmChooser = new VMCA_DumbVMChooser();
       EnableDisableTTPolicy enableDisablePolicy = new EDP_DeRecommissionTTs(actions.vc, actions.hd);
       //EnableDisableTTPolicy enableDisablePolicy = new EDP_SoftDeRecommissionTTs(vc, hd);
       //EnableDisableTTPolicy enableDisablePolicy = new EDP_JustPowerTTOnOff(vc);
       VHMConfig vhmc = new VHMConfig(vmChooser, enableDisablePolicy);

       EmbeddedVHM vhm = new EmbeddedVHM();
       vhm.init(vhmc, actions.vc, actions.mq);
       vhm.start();
   }
}
