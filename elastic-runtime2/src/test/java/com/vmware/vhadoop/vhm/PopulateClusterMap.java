package com.vmware.vhadoop.vhm;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToClusterMapper;
import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleEvent;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.SerengetiClusterVariableData;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.BootstrapMain;
import com.vmware.vhadoop.vhm.ClusterStateChangeListenerImpl;
import com.vmware.vhadoop.vhm.VHM;
import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor;
import com.vmware.vhadoop.vhm.rabbit.SimpleRabbitCredentials;
import com.vmware.vhadoop.vhm.strategy.DumbEDPolicy;
import com.vmware.vhadoop.vhm.strategy.DumbVMChooser;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;

public class PopulateClusterMap {
   VHM _vhm;
   ClusterStateChangeListenerImpl _cscl;
   MQClient _mqClient;

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
         public Set<ClusterScaleEvent> getImpliedScaleEventsForUpdate(SerengetiClusterVariableData vmd, String clusterId, boolean isNewCluster) {
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
