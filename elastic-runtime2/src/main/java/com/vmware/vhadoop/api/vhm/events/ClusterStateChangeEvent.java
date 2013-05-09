package com.vmware.vhadoop.api.vhm.events;

/* If the ClusterStateChangeListenerImpl notices a delta change, it generates one of these events for the VHM to consume */
public interface ClusterStateChangeEvent extends NotificationEvent {

   public class MasterVmEventData {
      public Boolean _enableAutomation;
      public Integer _minInstances;
      public Integer _jobTrackerPort;
   }
   
   public class VMEventData {
      // these two fields must always be filled
      public String _vmMoRef;
      public boolean _isLeaving;

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
   }
}
