package com.vmware.vhadoop.vhm;

import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.strategy.ScaleStrategy;
import com.vmware.vhadoop.model.Orchestrator;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.strategy.BalancedVMChooser;
import com.vmware.vhadoop.vhm.strategy.DumbEDPolicy;
import com.vmware.vhadoop.vhm.strategy.ManualScaleStrategy;
import com.vmware.vhadoop.vhm.vc.VcModelAdapter;

public class ModelController extends BootstrapMain {
   private static Logger _log = Logger.getLogger(ModelController.class.getName());

   Orchestrator orchestrator;

   public ModelController(Orchestrator orchestrator) {
      this.orchestrator = orchestrator;
   }

   public ModelController(final String configFileName, final String logFileName, Orchestrator orchestrator) {
      super(configFileName, logFileName);
      this.orchestrator = orchestrator;
   }

   @Override
   ScaleStrategy[] getScaleStrategies(final ThreadLocalCompoundStatus tlcs) {
      ScaleStrategy manualScaleStrategy = new ManualScaleStrategy(new BalancedVMChooser(), new DumbEDPolicy(getVCInterface(tlcs)));
      return new ScaleStrategy[] { manualScaleStrategy };
   }

   @Override
   VHM initVHM(final ThreadLocalCompoundStatus tlcs) {
      /* This next chunk is normally done out of BootstrapMain but we don't want most of what's needed to run a full system, so we register directly here */
      VHM vhm = new VHM(getVCInterface(tlcs), getScaleStrategies(tlcs), getStrategyMapper(), tlcs);
      ClusterStateChangeListenerImpl cscl = new ClusterStateChangeListenerImpl(getVCInterface(tlcs), orchestrator.getId());
      vhm.registerEventProducer(cscl);

      return vhm;
   }

   @Override
   public VCActions getVCInterface(ThreadLocalCompoundStatus tlcs) {
      return new VcModelAdapter(orchestrator);
   }
}
