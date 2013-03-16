package com.vmware.vhadoop.api.vhm;

import com.vmware.vhadoop.api.vhm.events.PropertyChangeEvent;
import com.vmware.vim.vmomi.client.Client;

public interface VCActions {

   public void changeVMPowerState(String vmMoRef, boolean b);

   public PropertyChangeEvent waitForPropertyChange(String folderName);
   
   public Client getStatsPollClient();

}
