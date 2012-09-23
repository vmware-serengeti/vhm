package com.vmware.vhadoop.vhm.vmcalgorithm;

import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public interface VMChooserAlgorithm {
   
   VMDTO[] chooseVMsToEnable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta);

   VMDTO[] chooseVMsToDisable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta);
}
