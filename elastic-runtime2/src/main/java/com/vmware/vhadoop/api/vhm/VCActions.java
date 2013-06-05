package com.vmware.vhadoop.api.vhm;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.vmware.vim.vmomi.client.Client;

/* Represents actions which can be invoked on the VC subsystem */
public interface VCActions {

   public static final String VC_POWER_ON_STATUS_KEY = "powerOnVM";
   public static final String VC_POWER_OFF_STATUS_KEY = "powerOffVM";

   public class MasterVmEventData {
      public Boolean _enableAutomation;
      public Integer _minInstances;
      public Integer _jobTrackerPort;
   }
   
   public class VMEventData {
      // these two fields must always be filled
      public String _vmMoRef;
      public Boolean _isLeaving;

      // these fields can be left as null if there is no new information
      /* TODO: Split into fields which are constant and variable and check that constants are not being changed */
      public Boolean _isElastic;
      public String _myName;
      public String _myUUID;
      public String _hostMoRef;
      public String _serengetiFolder;
      public String _masterUUID;
      public Boolean _powerState;
      public String _masterMoRef;
      public String _ipAddr;
      public String _dnsName;
      public Integer _vCPUs;
      
      /* If this is non-null, we derive that this is information about a master VM */
      public MasterVmEventData _masterVmData;
      
      @Override
      public String toString() {
         return "<%V"+_vmMoRef+"%V>, isLeaving="+_isLeaving+", isElastic="+_isElastic+", myName="+_myName+", myUUID="+_myUUID+", hostMoRef="+_hostMoRef+", serengetiFolder="+
                     _serengetiFolder+", masterUUID="+_masterUUID+", powerState="+_powerState+", masterMoRef="+_masterMoRef+", ipAddr="+_ipAddr+", dnsName="+_dnsName+", vCPUs="+_vCPUs+
                     ", masterVMData="+_masterVmData;
      }
   }
   
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs, boolean b);

   public String waitForPropertyChange(String folderName, String version, List<VMEventData> vmDataList) throws InterruptedException;
   
   public void interruptWait();
   
   public Client getStatsPollClient();

   public List<String> listVMsInFolder(String folderName);

}
