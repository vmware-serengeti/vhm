package com.vmware.vhadoop.vhm.vc;

import com.vmware.vhadoop.vhm.vc.VcCredentials;
import com.vmware.vhadoop.vhm.vc.VcVlsi;
import com.vmware.vim.vmomi.client.Client;

public class VcAdapter implements com.vmware.vhadoop.api.vhm.VCActions {
   private Client _cloneClient;   // used for stats polling and the main waitForPropertyChange loop
   private Client _defaultClient; // used for rest of VC operations
   private VcVlsi _vcVlsi;

   public VcAdapter(VcCredentials vcCreds) {
      _vcVlsi = new VcVlsi();
      try {
         _defaultClient = _vcVlsi.connect(vcCreds, true, false);
         _cloneClient = _vcVlsi.connect(vcCreds, true, true);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
   
   @Override
   public void changeVMPowerState(String vmMoRef, boolean b) {
      System.out.println(Thread.currentThread().getName()+": VCActions: changing power state of "+vmMoRef+" to "+b+"...");
      System.out.println(Thread.currentThread().getName()+": VCActions: ...done changing power state of "+vmMoRef);
   }

   @Override
   public com.vmware.vhadoop.api.vhm.events.PropertyChangeEvent waitForPropertyChange(String folderName) {
      _vcVlsi.testPC(_cloneClient, folderName);
      return null;
   }
   
   @Override
   public Client getStatsPollClient() {
      return _cloneClient;
   }

}
