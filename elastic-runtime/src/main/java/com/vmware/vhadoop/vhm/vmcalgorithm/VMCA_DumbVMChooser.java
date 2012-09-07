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
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.ProgressLogger;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public class VMCA_DumbVMChooser implements VMChooserAlgorithm {

   private static final String _className = VMCA_DumbVMChooser.class.getName();
   private static final ProgressLogger _pLog = ProgressLogger.getProgressLogger(_className);
   private static final Logger _log = _pLog.getLogger();

   @Override
   public VMCAResult chooseVMsToEnable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta) {
      CompoundStatus taskStatus = new CompoundStatus(_className+" enable");     /* TODO: Set the status somewhere */
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
      return new VMCAResult(toEnable.toArray(new VMDTO[0]), taskStatus);
   }

   @Override
   public VMCAResult chooseVMsToDisable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta) {
      CompoundStatus taskStatus = new CompoundStatus(_className+" disable");     /* TODO: Set the status somewhere */
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
      return new VMCAResult(toDisable.toArray(new VMDTO[0]), taskStatus);
   }

}
