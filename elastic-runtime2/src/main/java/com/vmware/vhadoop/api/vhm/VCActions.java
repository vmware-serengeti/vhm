package com.vmware.vhadoop.api.vhm;

import java.util.ArrayList;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.VMEventData;
import com.vmware.vim.vmomi.client.Client;

public interface VCActions {

   public void changeVMPowerState(String vmMoRef, boolean b);

   public String  waitForPropertyChange(String folderName, String version, ArrayList<VMEventData> vmDataList);
   
   public Client getStatsPollClient();

}
