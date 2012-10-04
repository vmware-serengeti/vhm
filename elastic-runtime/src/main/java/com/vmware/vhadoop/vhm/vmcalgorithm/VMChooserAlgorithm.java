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

import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.vhm.TTStatesForHost;

public interface VMChooserAlgorithm {
   
   public static class VMCAResult {
      private VMDTO[] _chosenVMs;
      private CompoundStatus _resultStatus;
      
      public VMCAResult(VMDTO[] chosenVMs, CompoundStatus resultStatus) {
         _chosenVMs = chosenVMs;
         _resultStatus = resultStatus;
      }

      public VMDTO[] getChosenVMs() {
         return _chosenVMs;
      }

      public CompoundStatus getChooserStatus() {
         return _resultStatus;
      }
   }
   
   VMCAResult chooseVMsToEnable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta);

   VMCAResult chooseVMsToDisable(TTStatesForHost[] hostAndVMs, int totalTTVMs, int delta);
}
