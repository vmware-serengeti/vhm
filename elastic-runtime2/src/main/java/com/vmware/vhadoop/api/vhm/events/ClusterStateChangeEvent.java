package com.vmware.vhadoop.api.vhm.events;

/* If the ClusterStateChangeListenerImpl notices a delta change, it generates one of these events for the VHM to consume */
public interface ClusterStateChangeEvent extends NotificationEvent {

   public enum VmType {
      MASTER, COMPUTE, OTHER;
   }
   
   public class VMConstantData {
      public VmType _vmType;
      public String _myUUID;
      
      public boolean isComplete() {
         return ((_vmType != null) && (_myUUID != null));
      }
   }

   public class VMVariableData {
      public String _myName;
      public Integer _vCPUs;
      public String _ipAddr;
      public String _dnsName;
      public Boolean _powerState;
      public String _hostMoRef;

      public boolean isComplete() {
         return ((_myName != null) && (_vCPUs != null) && (_ipAddr != null) && (_dnsName != null) && (_powerState != null) && (_hostMoRef != null));
      }
   }

   public class SerengetiClusterConstantData {
      public String _masterMoRef;
      public String _serengetiFolder;

      public boolean isComplete() {
         return ((_masterMoRef != null) && (_serengetiFolder != null));
      }
   }

   public class SerengetiClusterVariableData {
      public Boolean _enableAutomation;
      public Integer _minInstances;
      public Integer _jobTrackerPort;

      public boolean isComplete() {
         return ((_enableAutomation != null) && (_minInstances != null) && (_jobTrackerPort != null));
      }
   }

}
