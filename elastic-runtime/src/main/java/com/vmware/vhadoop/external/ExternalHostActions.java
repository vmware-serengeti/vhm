/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
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

package com.vmware.vhadoop.external;

import com.vmware.vhadoop.external.VCActionDTOTypes.HostDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMPowerState;

public interface ExternalHostActions {

   /* Callers can register to be notified if a VM is created or destroyed */
   public interface VMChangeEventCallback {
      public void vmCreated(String vmName, String hostName);

      public void vmDeleted(String vmName, String hostName);
   }

   public void powerOn();

   public void powerOff();

   public boolean powerOnVM(VMDTO vm);

   public boolean powerOffVM(VMDTO vm);

   public VMPowerState getVMPowerState(VMDTO vm);

   public boolean shutdownVM(VMDTO vm);

   public VMDTO deployVM(OVF ovf, String name);

   public boolean migrateTo(VMDTO vm, HostDTO migrateFrom);

   public long getFreeMemory();

   public String getName();
}
