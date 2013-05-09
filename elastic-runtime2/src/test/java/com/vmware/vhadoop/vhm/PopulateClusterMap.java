package com.vmware.vhadoop.vhm;

import java.util.Properties;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.ClusterMap.ExtraInfoToScaleStrategyMapper;
import com.vmware.vhadoop.api.vhm.MQClient;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
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

      ExtraInfoToScaleStrategyMapper strategyMapper = new ExtraInfoToScaleStrategyMapper() {
         @Override
         public String getStrategyKey(VMEventData vmd) {
            return ManualScaleStrategy.MANUAL_SCALE_STRATEGY_KEY;
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
