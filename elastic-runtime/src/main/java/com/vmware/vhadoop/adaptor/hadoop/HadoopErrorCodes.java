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

/**
 * Error codes here represent values which can be returned from execution of remote scripts
 * 
 */
public class HadoopErrorCodes {
   private Map<Integer, ErrorCode> _errorCodes;
   
   public static final int UNKNOWN_ERROR = -1;
   public static final int SUCCESS = 0;
   public static final int ERROR_BAD_ARGS = 100;
   public static final int ERROR_EXCLUDES_FILE_NOT_FOUND = 101;
   public static final int ERROR_DRLIST_FILE_NOT_FOUND = 102;
   public static final int ERROR_BAD_HADOOP_HOME = 103;
   public static final int ERROR_JT_CONNECTION = 104;
   public static final int ERROR_JT_UNKNOWN = 105;
   public static final int ERROR_FAIL_DERECOMMISSION = 106;
   public static final int ERROR_FEWER_TTS = 107;
   public static final int ERROR_EXCESS_TTS = 108;
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
      boolean _fatal;
      ParamTypes[] _params;

      public ErrorCode(int code, String logError, boolean fatal, ParamTypes[] params) {
         _code = code;
         _logError = logError;
         _fatal = fatal;
         _params = params;
      }
   }
   
   private void initErrorCodes() {
      addErrorCode(UNKNOWN_ERROR, true, "Unknown exit status from %s", DRSCRIPT);
      addErrorCode(SUCCESS, false, "Successfully executed %s script: %s", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_BAD_ARGS, true, "Bad arguments passed to %s", DRSCRIPT);
      addErrorCode(ERROR_EXCLUDES_FILE_NOT_FOUND, true, "Excludes file %s not found while executing %s", EXCLUDE_FILE, DRSCRIPT);
      addErrorCode(ERROR_DRLIST_FILE_NOT_FOUND, true, "%s list file %s not found while executing %s", COMMAND, DRLIST, DRSCRIPT);
      addErrorCode(ERROR_BAD_HADOOP_HOME, true, "HADOOP_HOME %s does not resolve correctly while running %s", HADOOP_HOME, DRSCRIPT);
      addErrorCode(ERROR_JT_CONNECTION, true, "Unable to connect to %s while running %s", JOBTRACKER, DRSCRIPT);
      addErrorCode(ERROR_JT_UNKNOWN, true, "Unknown error connecting to %s while running %s", JOBTRACKER, DRSCRIPT);
      addErrorCode(ERROR_FAIL_DERECOMMISSION, true, "Failed to %s one or more TTs while running %s", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_FEWER_TTS, true, "# Active TTs < Target number of TTs -- checked by %s", DRSCRIPT);
      addErrorCode(ERROR_EXCESS_TTS, true, "# Active TTs > Target number of TTs -- checked by %s", DRSCRIPT);
      addErrorCode(ERROR_COMMAND_NOT_EXECUTABLE, true, "%s script %s is not an executable", COMMAND, DRSCRIPT);
      addErrorCode(ERROR_COMMAND_NOT_FOUND, true, "Error in specifiying %s script %s", COMMAND, DRSCRIPT);
      addErrorCode(WARN_TT_EXCLUDESFILE, false, "One/More TTs were already %sed as per the excludes file %s while executing %s", COMMAND, EXCLUDE_FILE, DRSCRIPT);
      addErrorCode(WARN_TT_ACTIVE, false, "One/More TTs were already %sed as per \"hadoop job -list-active-trackers\"", COMMAND);
   }

   public void addErrorCode(int code, boolean fatal, String logError, ParamTypes... params) {
      _errorCodes.put(code, new ErrorCode(code, logError, fatal, params));
   }

   public boolean interpretErrorCode(Logger log, int code, Map<ParamTypes, String> paramValuesMap) {
      ErrorCode rc = _errorCodes.get(code);
      if (rc == null) {
         log.log(Level.WARNING, "Unknown error code!");
         rc = _errorCodes.get(-1);
      } else {
         if (paramValuesMap != null) {
            String[] paramValues = new String[rc._params.length];
            for (int i=0; i<paramValues.length; paramValues[i] = paramValuesMap.get(rc._params[i++]));
            log.log(Level.INFO, rc._logError, paramValues);
         } else {
            log.log(Level.INFO, rc._logError);
         }
      }
      return !rc._fatal;
   }
}
