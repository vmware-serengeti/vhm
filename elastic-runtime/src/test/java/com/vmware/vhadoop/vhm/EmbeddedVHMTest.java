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
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.vmware.vhadoop.adaptor.hadoop.HadoopAdaptor;
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
import com.vmware.vhadoop.vhm.EmbeddedVHM.VHMInputMessage;
import com.vmware.vhadoop.vhm.edpolicy.EDP_DeRecommissionTTs;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMCA_BalancedVMChooser;

public class EmbeddedVHMTest {
   EmbeddedVHM _test = new EmbeddedVHM();

   @Before
   public void init() {
      //System.setProperty("javax.net.debug", "ssl");         
      Logger.getLogger("").getHandlers()[0].setFormatter(new LogFormatter());
      VHMActions actions = MainController.setupActions();

      VHMConfig vhmc = new VHMConfig(new VMCA_BalancedVMChooser(), new EDP_DeRecommissionTTs(actions.vc, actions.hd));
      //VHMConfig vhmc = new VHMConfig(new VMCA_DumbVMChooser(), new EDP_DeRecommissionTTs(vc, hd));
      //VHMConfig vhmc = new VHMConfig(new VMCA_DumbVMChooser(), new EDP_JustPowerTTOnOff(vc));

      _test.init(vhmc, actions.vc, actions.mq);
   }
   
   @Test
   public void testWithFakeQueue() throws IOException {
//    String jsonMsg = "{\"version\":1,\"cluster_name\":\"cName\",\"jobtracker\":\"1.2.3.6\",\"instance_num\":4,\"node_groups\":[\"compute\"],\"serengeti_instance\":\"SERENGETI-vApp-xxx\"}";
      File file = new File("/tmp/vhm.json.msg");
      String jsonMsg = FileUtils.readFileToString(file);
      
      VHMInputMessage input = new VHMJsonInputMessage(jsonMsg.getBytes());
      _test.setNumTTVMsForCluster(input);
   }
   
//   @Test
   public void testWithRealQueue() {
      _test.start();
   }
}
