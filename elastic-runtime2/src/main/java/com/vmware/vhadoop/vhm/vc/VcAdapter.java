package com.vmware.vhadoop.vhm.vc;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.VMEventData;
import com.vmware.vhadoop.vhm.vc.VcCredentials;
import com.vmware.vhadoop.vhm.vc.VcVlsi;
import com.vmware.vim.vmomi.client.Client;

public class VcAdapter implements com.vmware.vhadoop.api.vhm.VCActions {
   private static final Logger _log = Logger.getLogger("VcAdapter");

   private Client _cloneClient;   // used for stats polling and the main waitForPropertyChange loop
   private Client _defaultClient; // used for rest of VC operations
   private VcVlsi _vcVlsi;
   private VcCredentials _vcCreds;

   
   private boolean initClients(boolean useCert) {
      try {
         _defaultClient = _vcVlsi.connect(_vcCreds, useCert, false);
         _cloneClient = _vcVlsi.connect(_vcCreds, useCert, true);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      if ((_defaultClient == null) || (_cloneClient == null)) {
         return false;
      }
      return true;
   }
   
   private void connect() {
      _vcVlsi = new VcVlsi();

      boolean useCert = false;
      if ((_vcCreds.keyStoreFile != null) && (_vcCreds.keyStorePwd != null) && (_vcCreds.vcExtKey != null)) {
         useCert = true;
      }
      boolean success = initClients(useCert);
      if (useCert && !success && (_vcCreds.user != null) && (_vcCreds.password != null)) { 
         _log.log(Level.WARNING, "Cert based login failed, trying user/password");
         initClients(false);
      }
   }
   
   // Reconnect to VC if connection timed out 
   private void validateConnection() {
      if (!_vcVlsi.testConnection()) {
         _log.log(Level.WARNING, "Found VC connection dropped; reconnecting");
         connect();
      }
   }
   
   public VcAdapter(VcCredentials vcCreds) {
      _vcCreds = vcCreds;
      connect();
   }
   
   @Override
   public void changeVMPowerState(String vmMoRef, boolean b) {
      validateConnection();
      System.out.println(Thread.currentThread().getName()+": VCActions: changing power state of "+vmMoRef+" to "+b+"...");
      System.out.println(Thread.currentThread().getName()+": VCActions: ...done changing power state of "+vmMoRef);
   }

   @Override
   public String  waitForPropertyChange(String folderName, String version, ArrayList<VMEventData> vmDataList) {
      validateConnection();
      return _vcVlsi.waitForUpdates(_cloneClient, folderName, version, vmDataList);
   }
   
   @Override
   public Client getStatsPollClient() {
      return _cloneClient;
   }

}
