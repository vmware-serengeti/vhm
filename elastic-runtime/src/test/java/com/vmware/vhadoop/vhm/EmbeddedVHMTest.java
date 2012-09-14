package com.vmware.vhadoop.vhm;

import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

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
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMInputMessage;
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMReturnMessage;
import com.vmware.vhadoop.vhm.edpolicy.EDP_DeRecommissionTTs;
import com.vmware.vhadoop.vhm.edpolicy.EDP_JustPowerTTOnOff;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMCA_DumbVMChooser;

public class EmbeddedVHMTest {
   EmbeddedVHM _test = new EmbeddedVHM();

   @Before
   public void init() {
      Logger.getLogger("").getHandlers()[0].setFormatter(new LogFormatter());
      VCUtils.trustAllHttpsCertificates();
      VCActions vc = new VCAdaptor(new SimpleVCCredentials("10.141.7.65", "Administrator", "ca$hc0w"));
      MQActions mq = new RabbitAdaptor(new SimpleRabbitCredentials("10.141.7.151", "bdd.runtime"));
      HadoopActions hd = new HadoopAdaptor(new SimpleHadoopCredentials("hduser", "had00p"), 
            new JTConfig("/usr/local/hadoop/hadoop-0.20.203.0", "/usr/local/hadoop/hadoop-0.20.203.0/conf/excludeTTs"));
      VHMConfig vhmc = new VHMConfig(new VMCA_DumbVMChooser(), new EDP_DeRecommissionTTs(vc, hd));
      _test.init(vhmc, vc, mq, hd);
   }
   
   @Test
   public void testWithFakeQueue() {
      String jsonMsg = "{\"version\":1,\"cluster_name\":\"DataComputeSplit0.20\",\"jobtracker\":\"10.140.109.105\",\"instance_num\":1,\"node_groups\":[\"TTVMs\"],\"serengeti_instance\":\"SERENGETI-xx\"}";
      VHMInputMessage input = new VHMJsonInputMessage(jsonMsg.getBytes());
      VHMReturnMessage output = _test.setNumTTVMsForCluster(input);
   }
   
//   @Test
   public void testWithRealQueue() {
      _test.start();
   }
}
