package com.vmware.vhadoop.vhm;

import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.hadoop.ModelHadoopAdapter;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti;
import com.vmware.vhadoop.vhm.rabbit.ModelRabbitAdaptor;
import com.vmware.vhadoop.vhm.vc.ModelVcAdapter;

public class ModelController extends BootstrapMain {
   private static Logger _log = Logger.getLogger(ModelController.class.getName());

   Serengeti vApp;

   public ModelController(Serengeti serengeti) {
      this.vApp = serengeti;
   }

   public ModelController(final String configFileName, final String logFileName, Serengeti serengeti) {
      super(configFileName, logFileName);
      this.vApp = serengeti;
   }

//   @Override
//   ScaleStrategy[] getScaleStrategies(final ThreadLocalCompoundStatus tlcs) {
//      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(new BalancedVMChooser(), new DumbEDPolicy(getVCInterface(tlcs)));
//      return new ScaleStrategy[] { manualScaleStrategy };
//   }
//
//   @Override
//   VHM initVHM(final ThreadLocalCompoundStatus tlcs) {
//      /* This next chunk is normally done out of BootstrapMain but we don't want most of what's needed to run a full system, so we register directly here */
//      VHM vhm = new VHM(getVCInterface(tlcs), getScaleStrategies(tlcs), getStrategyMapper(), tlcs);
//      ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(getVCInterface(tlcs), vApp.getId());
//      vhm.registerEventProducer(cscl);
//
//      return vhm;
//   }

   @Override
   public VCActions getVCInterface(ThreadLocalCompoundStatus tlcs) {
      return new ModelVcAdapter(vApp.getVCenter());
   }

   @Override
   HadoopActions getHadoopInterface() {
      return new ModelHadoopAdapter(vApp.getVCenter());
   }

   @Override
   ModelRabbitAdaptor getRabbitInterface() {
      return new ModelRabbitAdaptor(vApp.getVCenter());
   }
}
