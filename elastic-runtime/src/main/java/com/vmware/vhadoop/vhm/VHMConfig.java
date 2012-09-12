package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.vhm.edpolicy.EnableDisableTTPolicy;
import com.vmware.vhadoop.vhm.vmcalgorithm.VMChooserAlgorithm;

public class VHMConfig {
   VMChooserAlgorithm _vmChooser;
   EnableDisableTTPolicy _enableDisablePolicy;

   public VHMConfig(VMChooserAlgorithm vmc, EnableDisableTTPolicy edp) {
      _vmChooser = vmc;
      _enableDisablePolicy = edp;
   }
}
