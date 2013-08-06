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

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.vhm.rabbit.SimpleRabbitCredentials;
import com.vmware.vhadoop.vhm.strategy.DumbEDPolicy;
import com.vmware.vhadoop.vhm.strategy.DumbVMChooser;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;

public class PopulateClusterMap {
   VHM _vhm;
   ClusterStateChangeListenerImpl _cscl;
   MQClient _mqClient;

   @Ignore // disabled as this requires a configured VC connection with a live VC at the other end
   @Test
   public void test() {
      BootstrapMain mc = new BootstrapMain();
      VCActions vcActions = mc.getVCInterface(null);
      Properties properties = mc.getProperties();

      _mqClient = new RabbitAdaptor(new SimpleRabbitCredentials(
            properties.getProperty("msgHostName"),
            properties.getProperty("exchangeName"),
            properties.getProperty("routeKeyCommand"),
            properties.getProperty("routeKeyStatus")));

      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(
            new DumbVMChooser(), new DumbEDPolicy(vcActions));

      ExtraInfoToClusterMapper strategyMapper = new ExtraInfoToClusterMapper() {
         @Override
         public String getStrategyKey(SerengetiClusterVariableData vmd, String clusterId) {
            return ManualScaleStrategy.MANUAL_SCALE_STRATEGY_KEY;
         }

         @Override
         public Map<String, String> parseExtraInfo(SerengetiClusterVariableData vmd, String clusterId) {
            return null;
         }

         @Override
         public Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData vmd, String clusterId, boolean isNewCluster, boolean isClusterViable) {
            return null;
         }
      };

      _vhm = new VHM(vcActions, new ScaleStrategy[] { manualScaleStrategy },
            strategyMapper, new ThreadLocalCompoundStatus());
      _cscl = new ClusterStateChangeListenerImpl(_vhm.getVCActions(),
            properties.getProperty("uuid"));

      _vhm.registerEventProducer(_cscl);
      _vhm.registerEventProducer(_mqClient);

      _vhm.start();

      try {
         Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
}
