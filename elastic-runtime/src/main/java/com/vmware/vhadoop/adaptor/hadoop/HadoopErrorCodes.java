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

package com.vmware.vhadoop.adaptor.hadoop;

import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes.COMMAND;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes.DRLIST;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes.DRSCRIPT;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes.EXCLUDE_FILE;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes.HADOOP_HOME;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes.JOBTRACKER;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.util.CompoundStatus;

/**
 * Error codes here represent values which can be returned from execution of remote scripts
 * 
 */
public class HadoopErrorCodes {
   private Map<Integer, ErrorCode> _errorCodes;
   
   public static final int UNKNOWN_ERROR = -1;
   public static final int SUCCESS = 0;
   public static final int ERROR_CATCHALL = 1;
   public static final int ERROR_BAD_ARGS = 100;
   public static final int ERROR_EXCLUDES_FILE_NOT_FOUND = 101;
   public static final int ERROR_DRLIST_FILE_NOT_FOUND = 102;
   public static final int ERROR_BAD_HADOOP_HOME = 103;
   public static final int ERROR_JT_CONNECTION = 104;
   public static final int ERROR_JT_UNKNOWN = 105;
   public static final int ERROR_FAIL_DERECOMMISSION = 106;
   public static final int ERROR_FEWER_TTS = 107;
   public static final int ERROR_EXCESS_TTS = 108;
   public static final int ERROR_BAD_TARGET_TTS=109;
   public static final int ERROR_EXCLUDES_FILE_UPDATE = 110;
   public static final int ERROR_LOCK_FILE_WRITE = 111;
   public static final int ERROR_COMMAND_NOT_EXECUTABLE = 126;
   public static final int ERROR_COMMAND_NOT_FOUND = 127;
   public static final int WARN_TT_EXCLUDESFILE = 200;
   public static final int WARN_TT_ACTIVE = 201;

   /* These are the parameter names of variables that can be printed in the log messages */
   public static enum ParamTypes{COMMAND, DRSCRIPT, EXCLUDE_FILE, DRLIST, HADOOP_HOME, JOBTRACKER};
   
   public HadoopErrorCodes() {
      _errorCodes = new HashMap<Integer, ErrorCode>();
      initErrorCodes();
   }
   
   class ErrorCode {
      int _code;
      String _logError;
      boolean _isMajor;
      ParamTypes[] _params;

      public ErrorCode(int code, String logError, boolean isMajor, ParamTypes[] params) {
         _code = code;
         _logError = logError;
         _isMajor = isMajor;
         _params = params;
      }
   }
   
   private void initErrorCodes() {
      addErrorCode(UNKNOWN_ERROR, true, "Unknown exit status during %s;", COMMAND);
      addErrorCode(SUCCESS, false, "Successfully executed %s script (%s);", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_CATCHALL, true, "Generic error while executing %s", COMMAND);
      addErrorCode(ERROR_BAD_ARGS, true, "Bad arguments passed to %s script (%s)", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_EXCLUDES_FILE_NOT_FOUND, true, "Excludes file (%s) not found while executing %s script (%s);", EXCLUDE_FILE, COMMAND, DRSCRIPT);
      addErrorCode(ERROR_DRLIST_FILE_NOT_FOUND, true, "%s list file (%s) not found while executing %s script (%s);", COMMAND, DRLIST, COMMAND, DRSCRIPT);
      addErrorCode(ERROR_BAD_HADOOP_HOME, true, "HADOOP_HOME (%s) does not resolve correctly while running %s script (%s);", HADOOP_HOME, COMMAND, DRSCRIPT);
      addErrorCode(ERROR_JT_CONNECTION, true, "Unable to connect to jobtracker (%s) while running %s script (%s);", JOBTRACKER, COMMAND, DRSCRIPT);
      addErrorCode(ERROR_JT_UNKNOWN, true, "Unknown error connecting to jobtracker (%s) while running %s script (%s);", JOBTRACKER, COMMAND, DRSCRIPT);
      addErrorCode(ERROR_FAIL_DERECOMMISSION, true, "Failed to %s one or more TTs while running %s script (%s);", COMMAND, COMMAND, DRSCRIPT);
      addErrorCode(ERROR_FEWER_TTS, true, "# Active TTs < Target number of TTs -- checked by validator script (%s);", DRSCRIPT);
      addErrorCode(ERROR_EXCESS_TTS, true, "# Active TTs > Target number of TTs -- checked by validator script (%s);", DRSCRIPT);
      addErrorCode(ERROR_COMMAND_NOT_EXECUTABLE, true, "%s script (%s) is not an executable;", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_COMMAND_NOT_FOUND, true, "Error in specifiying %s script (%s) - Command not found;", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_BAD_TARGET_TTS, true, "Bad number of target TTs specified while executing %s script (%s);", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_EXCLUDES_FILE_UPDATE, true, "Error while trying to update excludes file during %sing (wrong permissions/user perhaps?);", COMMAND);
      addErrorCode(ERROR_LOCK_FILE_WRITE, true, "Error while trying to write to lock file during %sing (wrong permissions/user perhaps?);", COMMAND);
      addErrorCode(WARN_TT_EXCLUDESFILE, false, "One/More TTs were already %sed as per the excludes file (%s) while executing %s;", COMMAND, EXCLUDE_FILE, DRSCRIPT);
      addErrorCode(WARN_TT_ACTIVE, false, "One/More TTs were already %sed as per \"hadoop job -list-active-trackers\";", COMMAND);
   }

   public void addErrorCode(int code, boolean isMajor, String logError, ParamTypes... params) {
      _errorCodes.put(code, new ErrorCode(code, logError, isMajor, params));
   }

   public CompoundStatus interpretErrorCode(Logger log, int code, Map<ParamTypes, String> paramValuesMap) {
      CompoundStatus status = new CompoundStatus("interpretErrorCode");
      ErrorCode rc = _errorCodes.get(code);
      String[] paramValues = null;
      if (rc == null) {
         log.log(Level.WARNING, "Unknown error code!");
         rc = _errorCodes.get(-1);
      } else {
         if (paramValuesMap != null) {
            paramValues = new String[rc._params.length];
            for (int i=0; i<paramValues.length; paramValues[i] = paramValuesMap.get(rc._params[i++]));
            log.log(Level.INFO, rc._logError, paramValues);
         } else {
            log.log(Level.INFO, rc._logError);
         }
      }
      if (rc._code == SUCCESS) {
         status.registerTaskSucceeded();
      } else if (rc._isMajor) {
         String formattedString = (paramValues == null) ?
               rc._logError : String.format(rc._logError, (Object[])paramValues);
         /* Currently all of these errors are considered non-fatal to subsequent operations */
         status.registerTaskFailed(false, formattedString);
      }
      return status;
   }
}
