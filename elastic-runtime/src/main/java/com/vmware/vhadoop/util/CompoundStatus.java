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
   
   public List<TaskStatus> _taskStatusList;     /* TODO: Order by timestamp for audit */
   public String _name;
   
   public static class TaskStatus {
      public enum TaskState{SUCCEEDED, INCOMPLETE, FAILED};
      private String _compoundName;
      private final long _timeOccurred = System.currentTimeMillis();
      private TaskState _taskState;
      private boolean _isFatal;
      private String _message;
      
      public TaskStatus(TaskState state, boolean isFatal, String message, String compoundName) {
         _taskState = state;
         _isFatal = isFatal;
         _message = message;
         _compoundName = compoundName;
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
      _taskStatusList.add(new TaskStatus(TaskState.SUCCEEDED, false, null, _name));
   }
   
   /* If a task fails, record the failure. It could be fatal or not */
   public void registerTaskFailed(boolean isFatal, String errorMsg) {
      _taskStatusList.add(new TaskStatus(TaskState.FAILED, false, errorMsg, _name));
   }
   
   /* If a task doesn't complete, this could be indication for a retry */
   public void registerTaskIncomplete(boolean isFatal, String errorMsg) {
      _taskStatusList.add(new TaskStatus(TaskState.INCOMPLETE, false, errorMsg, _name));
   }
   
   public int getTotalTaskCount() {
      return _taskStatusList.size();
   }
   
   public int getFailedTaskCount() {
      int result = 0;
      for (TaskStatus status : _taskStatusList) {
         if (status.getTaskState().equals(TaskState.FAILED)) {
            result++;
         }
      }
      return result;
   }
   
   public int getIncompleteTaskCount() {
      int result = 0;
      for (TaskStatus status : _taskStatusList) {
         if (status.getTaskState().equals(TaskState.INCOMPLETE)) {
            result++;
         }
      }
      return result;
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

   /* Utility method to look through the array of CompoundStatuses and find the failure with the earliest timestamp */
   public static TaskStatus getFirstFailure(CompoundStatus[] statuses) {
      TaskStatus result = null;
      for (CompoundStatus compStatus : statuses) {
         for (TaskStatus taskStatus : compStatus._taskStatusList) {
            if (!taskStatus._taskState.equals(TaskState.SUCCEEDED) && (taskStatus._message != null)) {
               if (result == null) {
                  result = taskStatus;
               } else if (taskStatus._timeOccurred < result._timeOccurred) {
                  result = taskStatus;
               }
            }
         }
      }
      return result;
   }
}
