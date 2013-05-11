package com.vmware.vhadoop.api.vhm;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vim.vmomi.client.Client;

/* Represents actions which can be invoked on the VC subsystem */
public interface VCActions {

   public static final String VC_POWER_ON_STATUS_KEY = "powerOnVM";
   public static final String VC_POWER_OFF_STATUS_KEY = "powerOffVM";

   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean b);

   public String waitForPropertyChange(String folderName, String version, List<VMEventData> vmDataList) throws InterruptedException;
   
   public void interruptWait();
   
   public Client getStatsPollClient();

   public List<String> listVMsInFolder(String folderName);

}
