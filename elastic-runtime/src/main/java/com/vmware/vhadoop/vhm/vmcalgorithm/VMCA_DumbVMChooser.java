/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
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
