/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
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

package com.vmware.vhadoop.util;

import java.util.ArrayList;
import java.util.List;
import com.vmware.vhadoop.util.CompoundStatus.TaskStatus.TaskState;

/**
 * CompoundStatus is a class used to allow a method to register successes or failures in its operation
 *   and then return the status to the caller which can then combine it with its own status.
 * In this way, an audit trail of statuses can be build up over a call stack and can then finally be
 *   analyzed by the initial caller which decides what to do about the results
 * Note that the caller should not assume that every successful method in the stack above will 
 *   necessarily have called registerTaskSuccess(). This is why the final focus is on how many failures there were.
 * Each CompoundStatus has a name and each TaskStatus created within it gets tagged with that name
 *   Hence when compoundStatus objects are combined with each other, it is clear which operation registered what.
 * "Fatal" here means that the entire operation should be subsequently aborted - there is no point continuing
 * 
 * @author bcorrie
 *
 */
public class CompoundStatus {
   
   private List<TaskStatus> _taskStatusList;     /* Ordered by timestamp for audit */
   private String _name;
   
   public static class TaskStatus {
      public enum TaskState{SUCCEEDED, INCOMPLETE, FAILED};
      private String _compoundName;
      private final long _timeOccurred = System.currentTimeMillis();
      private TaskState _taskState;
      private boolean _isFatal;
      private String _message;
      private Integer _errorCode;
      
      public TaskStatus(TaskState state, boolean isFatal, String message, String compoundName, Integer errorCode) {
         _taskState = state;
         _isFatal = isFatal;
         _message = message;
         _compoundName = compoundName;
         _errorCode = errorCode;
      }
      
      public TaskState getTaskState() {
         return _taskState;
      }
      
      public boolean getIsFatal() {
         return _isFatal;
      }
      
      public String getMessage() {
         return _message;
      }
      
      public long getTimeOccurred() {
         return _timeOccurred;
      }
      
      public String getCompoundName() {
         return _compoundName;
      }
      
      public Integer getErrorCode() {
         return _errorCode;
      }

      @Override
      public String toString() {
         return _timeOccurred+": "+_compoundName+" -> "+_message;
      }
   }
   
   /* Create a new compound status. The name is typically the class/method name that created the status */
   public CompoundStatus(String name) {
      _taskStatusList = new ArrayList<TaskStatus>();
      _name = name;
   }
   
   /* Incorporate the input status into this status */
   public void addStatus(CompoundStatus statusToAdd) {
      _taskStatusList.addAll(statusToAdd._taskStatusList);
   }
   
   /* If a task succeeds, provide a record of this */
   public void registerTaskSucceeded() {
      _taskStatusList.add(new TaskStatus(TaskState.SUCCEEDED, false, null, _name, null));
   }

   public void registerTaskFailed(boolean isFatal, String errorMsg) {
      registerTaskFailed(isFatal, errorMsg, null);
   }
   
   /* If a task fails, record the failure. It could be fatal or not */
   public void registerTaskFailed(boolean isFatal, String errorMsg, Integer errorCode) {
      _taskStatusList.add(new TaskStatus(TaskState.FAILED, false, errorMsg, _name, errorCode));
   }

   public void registerTaskIncomplete(boolean isFatal, String errorMsg) {
      registerTaskIncomplete(isFatal, errorMsg, null);
   }
   
   /* If a task doesn't complete, this could be indication for a retry */
   public void registerTaskIncomplete(boolean isFatal, String errorMsg, Integer errorCode) {
      _taskStatusList.add(new TaskStatus(TaskState.INCOMPLETE, false, errorMsg, _name, errorCode));
   }
   
   public int getTotalTaskCount() {
      return _taskStatusList.size();
   }
   
   private int getTaskCountForState(TaskState state) {
      int result = 0;
      for (TaskStatus status : _taskStatusList) {
         if (status.getTaskState().equals(state)) {
            result++;
         }
      }
      return result;
   }
   
   public int getFailedTaskCount() {
      return getTaskCountForState(TaskState.FAILED);
   }
   
   public int getIncompleteTaskCount() {
      return getTaskCountForState(TaskState.INCOMPLETE);
   }
   
   public int getFatalFailureCount() {
      int result = 0;
      for (TaskStatus status : _taskStatusList) {
         if (status.getIsFatal()) {
            result++;
         }
      }
      return result;
   }

   public TaskStatus getFirstFailure() {
      return getFirstFailure(null);
   }

   public TaskStatus getFirstFailure(String failedCompoundName) {
      TaskStatus result = null;
      for (TaskStatus taskStatus : _taskStatusList) {
         if (((failedCompoundName == null) || taskStatus._compoundName.equals(failedCompoundName)) &&
               !taskStatus._taskState.equals(TaskState.SUCCEEDED)) {
            if ((result == null) || result._timeOccurred > taskStatus._timeOccurred) {
               result = taskStatus;
            }
         }
      }
      return result;
   }

   /* true == no failure found */
   public boolean screenStatusesForSpecificFailures(String[] failedCompoundNames) {
      for (TaskStatus taskStatus : _taskStatusList) {
         for (String failureName : failedCompoundNames) {
            if (taskStatus._compoundName.equals(failureName) && 
                  !taskStatus._taskState.equals(TaskState.SUCCEEDED)) {
               return false;
            }
         }
      }
      return true;
   }
}
