package com.vmware.vhadoop.api.vhm;

import com.vmware.vhadoop.api.vhm.events.NotificationEvent;

public interface ClusterStateChangeEvent extends NotificationEvent {

   public abstract class VMEventData {
      public String _vmMoRef;
      public String _hostMoRef;
      public String _serengetiUUID;
      public String _masterUUID;
      public boolean _powerState;
   }
   
   public class ComputeVMEventData extends VMEventData {
      public String _masterMoRef;
   }
   
   public class MasterVMEventData extends VMEventData {
      public boolean _enableAutomation;
      public int _minInstances;
   }
}
