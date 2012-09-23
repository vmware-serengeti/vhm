package com.vmware.vhadoop.vhm.vmcalgorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public class VMCA_DumbVMChooser implements VMChooserAlgorithm {

   private static final Logger _log = Logger.getLogger(VMCA_DumbVMChooser.class.getName());

   @Override
   public VMDTO[] chooseVMsToEnable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta) {
      List<VMDTO> toEnable = new ArrayList<VMDTO>();
      /* Just cycle through the hosts taking as many VMs as each will give */
      for (TTStatesForHost hostAndVM : hostAndVMs) {
         int remaining = delta - toEnable.size();
         if (remaining <= 0) {
        	 break;
         }
         for (VMDTO vm : hostAndVM.getDisabled()) {
             toEnable.add(vm);
        	 remaining--;
        	 if (remaining <= 0) {
        	    break;
        	 }
         }
      }
      if (delta > toEnable.size()) {
         _log.severe("Request to enable more VMs than are available!");
      }
      return toEnable.toArray(new VMDTO[0]);
   }

   @Override
   public VMDTO[] chooseVMsToDisable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta) {
      List<VMDTO> toDisable = new ArrayList<VMDTO>();
      /* Just cycle through the hosts taking as many VMs as each will give */
      for (TTStatesForHost hostAndVM : hostAndVMs) {
         int remaining = delta - toDisable.size();
         if (remaining <= 0) {
        	 break;
         }
         for (VMDTO vm: hostAndVM.getEnabled()) {
             toDisable.add(vm);
             remaining--;
             if (remaining <= 0) {
                	break;
             }
         }
      }
      if (delta > toDisable.size()) {
         _log.severe("Request to disable more VMs than are available!");
      }
      return toDisable.toArray(new VMDTO[0]);
   }

}
