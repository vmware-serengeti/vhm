package com.vmware.vhadoop.vhm.vmcalgorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public class VMCA_DumbVMChooser implements VMChooserAlgorithm {

   private static final Logger _log = Logger.getLogger(VMCA_DumbVMChooser.class.getName());

   @Override
   public VMDTO[] chooseVMsToEnable(TTStatesForHost[] hostAndVMs, int delta) {
      List<VMDTO> toEnable = new ArrayList<VMDTO>();
      /* Just cycle through the hosts taking as many VMs as each will give */
      for (TTStatesForHost hostAndVM : hostAndVMs) {
         int remaining = delta - toEnable.size();
         if (remaining > 0) {
            for (int i=0; i<remaining; i++) {
               if (hostAndVM.getDisabled().length == i) {
                  break;
               }
               toEnable.add(hostAndVM.getDisabled()[i]);
            }
         }
      }
      if (delta > toEnable.size()) {
         _log.severe("Request to enable more VMs than are available!");
      }
      return toEnable.toArray(new VMDTO[0]);
   }

   @Override
   public VMDTO[] chooseVMsToDisable(TTStatesForHost[] hostAndVMs, int delta) {
      List<VMDTO> toDisable = new ArrayList<VMDTO>();
      /* Just cycle through the hosts taking as many VMs as each will give */
      for (TTStatesForHost hostAndVM : hostAndVMs) {
         int remaining = delta - toDisable.size();
         if (remaining > 0) {
            for (int i=0; i<remaining; i++) {
               if (hostAndVM.getEnabled().length == i) {
                  break;
               }
               toDisable.add(hostAndVM.getEnabled()[i]);
            }
         }
      }
      if (delta > toDisable.size()) {
         _log.severe("Request to disable more VMs than are available!");
      }
      return toDisable.toArray(new VMDTO[0]);
   }

}
