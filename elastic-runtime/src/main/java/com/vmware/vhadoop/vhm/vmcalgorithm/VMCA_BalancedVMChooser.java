package com.vmware.vhadoop.vhm.vmcalgorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public class VMCA_BalancedVMChooser implements VMChooserAlgorithm {

   private static final Logger _log = Logger.getLogger(VMCA_DumbVMChooser.class.getName());
   
   @Override
   public VMDTO[] chooseVMsToEnable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta) {
	  List<VMDTO> toEnable = new ArrayList<VMDTO>();
	  /* Choose TT VMs for power-on, keeping/improving per host balance by increasing powered-on 
	   * TT VMs on hosts in order of least to most running VMs.
	   */
      for (int targetPerHost = 0; targetPerHost <= totalTTVMs; targetPerHost++) {
         int remaining = delta - toEnable.size();
         if (remaining <= 0) {
         	 break;
         }
         for (TTStatesForHost host: hostAndVMs) {
             if (host.getDisabled().size() <= 0) {
        	     continue;
        	 }
             if (host.getEnabled().size() == targetPerHost) {
            	 VMDTO vm = host.getDisabled().get(0);
                 toEnable.add(vm);
            	 host.getDisabled().remove(0);
            	 host.getEnabled().add(vm);
                 remaining--;
                 if (remaining <= 0) {
            	     break;
                 }
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
	  /* Choose TT VMs for power-off, keeping/improving per host balance by decreasing powered-on 
	   * TT VMs on hosts in order of most to least running VMs.
	   */
      for (int targetPerHost = totalTTVMs; targetPerHost >= 0; targetPerHost--) {
         int remaining = delta - toDisable.size();
         if (remaining <= 0) {
         	 break;
         }
         for (TTStatesForHost host: hostAndVMs) {
             if (host.getEnabled().size() <= 0) {
        	     continue;
        	 }
             if (host.getEnabled().size() == targetPerHost) {
            	 VMDTO vm = host.getEnabled().get(0);
                 toDisable.add(vm);
            	 host.getEnabled().remove(0);
            	 host.getDisabled().add(vm);
                 remaining--;
                 if (remaining <= 0) {
            	     break;
                 }
             }
         }
      }
      
      if (delta > toDisable.size()) {
         _log.severe("Request to disable more VMs than are available!");
      }
      return toDisable.toArray(new VMDTO[0]);
   }

}
