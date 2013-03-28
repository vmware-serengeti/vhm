package com.vmware.vhadoop.api.vhm;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;
import com.vmware.vim.vmomi.client.Client;

public interface VCActions {

   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean b);

   public String waitForPropertyChange(String folderName, String version, List<VMEventData> vmDataList);
   
   public Client getStatsPollClient();

   public List<String> listVMsInFolder(String folderName);

}
