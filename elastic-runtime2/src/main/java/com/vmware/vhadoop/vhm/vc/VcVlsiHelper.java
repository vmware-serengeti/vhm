package com.vmware.vhadoop.vhm.vc;

import static com.vmware.vhadoop.vhm.vc.VcVlsi.VHM_EXTRA_CONFIG_AUTOMATION_ENABLE;
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

   static void parseExtraConfig(VMEventData vmData, String key, String value) {
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
            getMasterVmData(vmData)._minInstances = Integer.valueOf(value);
         } else if (key.equals(VHM_EXTRA_CONFIG_JOB_TRACKER_PORT)) {
            getMasterVmData(vmData)._jobTrackerPort = Integer.valueOf(value);
         }
      }
   }
}