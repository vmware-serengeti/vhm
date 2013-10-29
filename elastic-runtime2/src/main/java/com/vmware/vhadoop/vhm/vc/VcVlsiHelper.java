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

package com.vmware.vhadoop.vhm.vc;

import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_AUTOMATION_ENABLE;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_AUTOMATION_INSTANCE_RANGE;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_AUTOMATION_MIN_INSTANCES;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_ELASTIC;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_JOB_TRACKER_PORT;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_MASTER_MOREF;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_MASTER_UUID;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_PREFIX;
import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_UUID;

import com.vmware.vhadoop.api.vhm.VCActions.MasterVmEventData;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;

/**
 * Splits out those portions that the model requires so that it doesn't have a vmodl dependency
 */
public class VcVlsiHelper {
   static private MasterVmEventData getMasterVmData(VMEventData vmData) {
      if (vmData._masterVmData == null) {
         vmData._masterVmData = new MasterVmEventData();
      }
      return vmData._masterVmData;
   }

   static void parseExtraConfig(VMEventData vmData, String key, String value) throws NumberFormatException {
      if (key.startsWith(VHM_EXTRA_CONFIG_PREFIX)) {
         //_log.log(Level.INFO, "PEC key:val = " + key + " : " + value);
         if (key.equals(VHM_EXTRA_CONFIG_UUID)) {
            vmData._serengetiFolder = value;
         } else if (key.equals(VHM_EXTRA_CONFIG_MASTER_UUID)) {
            vmData._masterUUID = value;
         } else if (key.equals(VHM_EXTRA_CONFIG_MASTER_MOREF)) {
            vmData._masterMoRef = value;
         } else if (key.equals(VHM_EXTRA_CONFIG_ELASTIC)) {
            vmData._isElastic = value.equalsIgnoreCase("true");
         } else if (key.equals(VHM_EXTRA_CONFIG_AUTOMATION_ENABLE)) {
            getMasterVmData(vmData)._enableAutomation = value.equalsIgnoreCase("true");
         } else if (key.equals(VHM_EXTRA_CONFIG_AUTOMATION_MIN_INSTANCES)) {
            /* Maintained for backwards compatibility - max is always unset */
            if (getMasterVmData(vmData)._minInstances == null) {        /* Only if not been set by the M7 code below */
               getMasterVmData(vmData)._minInstances = Integer.valueOf(value);
               getMasterVmData(vmData)._maxInstances = -1;
            }
         } else if (key.equals(VHM_EXTRA_CONFIG_AUTOMATION_INSTANCE_RANGE)) {
            int separatorIndex = value.indexOf(':');
            if ((separatorIndex < 1) || (value.length() < 3)) {
               throw new NumberFormatException("Format for VC key "+VHM_EXTRA_CONFIG_AUTOMATION_INSTANCE_RANGE+" is wrong: "+value);
            }
            getMasterVmData(vmData)._minInstances = Integer.parseInt(value.substring(0, separatorIndex));
            getMasterVmData(vmData)._maxInstances = Integer.parseInt(value.substring(separatorIndex+1, value.length()));;
         } else if (key.equals(VHM_EXTRA_CONFIG_JOB_TRACKER_PORT)) {
            getMasterVmData(vmData)._jobTrackerPort = Integer.valueOf(value);
         }
      }
   }
}