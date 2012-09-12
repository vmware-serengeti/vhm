package com.vmware.vhadoop.vhm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import com.vmware.vhadoop.adaptor.hadoop.HadoopAdaptor;
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
import com.vmware.vhadoop.vhm.edpolicy.EDP_JustPowerTTOnOff;
import com.vmware.vhadoop.vhm.edpolicy.EnableDisableTTPolicy;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMCA_DumbVMChooser;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMChooserAlgorithm;

public class MainController {
   static Properties properties = null;
   
   public static void readPropertiesFile(String fileName) {
       try {
           File file = new File(fileName);
           FileInputStream fileInput = new FileInputStream(file);
           properties = new Properties();
           properties.load(fileInput);
           fileInput.close();
       } catch (FileNotFoundException e) {
           e.printStackTrace();
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

   public static void main(String[] args) {
       Logger.getLogger("").getHandlers()[0].setFormatter(new LogFormatter());
       VCUtils.trustAllHttpsCertificates();         /* TODO: BAD??? */
       readPropertiesFile("/tmp/vHadoopProperties");
       
       /* TODO: As we build these subsystems, we should be checking that they're operational
        * and putting in decent error handling if they're not */

       VCActions vc = new VCAdaptor(new SimpleVCCredentials(
             properties.getProperty("vCenterId"), 
             properties.getProperty("vCenterUser"),
             properties.getProperty("vCenterPwd")));
       
       VMChooserAlgorithm vmChooser = new VMCA_DumbVMChooser();
       EnableDisableTTPolicy enableDisablePolicy = new EDP_JustPowerTTOnOff(vc);
       
       MQActions mq = new RabbitAdaptor(
             new SimpleRabbitCredentials(
                   properties.getProperty("msgHostName"),
                   properties.getProperty("exchangeName")));

       HadoopActions hd = new HadoopAdaptor(
             new SimpleHadoopCredentials(
                   properties.getProperty("vHadoopId"), 
                   properties.getProperty("vHadoopPwd")), 
             new JTConfig(
                   properties.getProperty("vHadoopHome"),
                   properties.getProperty("vHadoopExcludeTTFile")));
       
       VHMConfig vhmc = new VHMConfig(vmChooser, enableDisablePolicy);

       EmbeddedVHM vhm = new EmbeddedVHM();
       vhm.init(vhmc, vc, mq, hd);
       vhm.start();
       /* TODO: Since start is blocking in an infinite loop, no point having any code here
        * Consider having the ability to cleanly stop or interrupt vhm in a separate thread
        * There is already vhm.stop() for that purpose */
   }
}
