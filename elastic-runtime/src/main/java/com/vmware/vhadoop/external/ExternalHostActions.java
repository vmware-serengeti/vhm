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
