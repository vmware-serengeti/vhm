/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

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
      
      protected String getVariableValues() {
         return "type="+(_vmType==null ? null : _vmType.name())+", UUID="+_myUUID;
      }
      
      @Override
      public String toString() {
         return "VMConstantData{"+getVariableValues()+"}";
      }
   }

   public class VMVariableData {
      public String _myName;
      public Integer _vCPUs;
      public String _ipAddr;
      public String _dnsName;
      public Boolean _powerState;
      public String _hostMoRef;

      protected String getVariableValues() {
         return "name="+_myName+", vCPUs="+_vCPUs+", ipAddr="+_ipAddr+", dnsName="+_dnsName+", powerState="+_powerState+", hostMoRef="+_hostMoRef;
      }

      @Override
      public String toString() {
         return "VMVariableData{"+getVariableValues()+"}";
      }
   }

   public class SerengetiClusterConstantData {
      public String _masterMoRef;
      public String _serengetiFolder;

      public boolean isComplete() {
         return ((_masterMoRef != null) && (_serengetiFolder != null));
      }

      @Override
      public String toString() {
         return "SerengetiClusterConstantData{masterMoRef="+_masterMoRef+", folder="+_serengetiFolder+"}";
      }
   }

   public class SerengetiClusterVariableData {
      public Boolean _enableAutomation;
      public Integer _minInstances;
      public Integer _maxInstances;
      public Integer _jobTrackerPort;

      public boolean isComplete() {
         return ((_enableAutomation != null) && (_minInstances != null) && (_maxInstances != null) && (_jobTrackerPort != null));
      }

      @Override
      public String toString() {
         return "SerengetiClusterVariableData{auto="+_enableAutomation+", minInstances="+_minInstances+", maxInstances="+_maxInstances+", jobTrackerPort="+_jobTrackerPort+"}";
      }
   }

}
