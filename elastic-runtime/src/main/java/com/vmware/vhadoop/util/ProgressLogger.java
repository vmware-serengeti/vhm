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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ProgressLogger is a class for logging progress with a ProgressReporter.
 * It is designed to integrate with the java.util.logging.Logger used by a class.
 * Usage: If a class wants to use a ProgressLogger, it should call ProgressLogger.getProgressLogger()
 *   and then get the Logger instance out of that for regular logging: eg. _progressLogger.getLogger();
 *   
 * This class cannot currently be used by multiple threads
 *   
 * @author bcorrie
 *
 */
public class ProgressLogger {
   
   public interface ProgressReporter {
      public void reportProgress(int percentage, String message);
   }
   
   private Logger _logger;
   private static Map<String, ProgressLogger> _progressLoggers = new HashMap<String, ProgressLogger>();
   private static ProgressReporter _progressReporter;
   
   /* Note that the ProgressLogger is configured statically to follow the convention of Logger
    * Because of this, the ProgressReporter may need to be added after the ProgressLogger instances
    *   have been created. This is fine, since each ProgressLogger will use the same ProgressReporter instance.
    */
   public static void setProgressReporter(ProgressReporter reporter) {
      _progressReporter = reporter;
   }
   
   private ProgressLogger(Logger logger) {
      _logger = logger;
   }

   public static ProgressLogger getProgressLogger(String name) {
      ProgressLogger result = _progressLoggers.get(name);
      if (result == null) {
         result = new ProgressLogger(Logger.getLogger(name));
         _progressLoggers.put(name, result);
      }
      return result;
   }
  
   public void registerProgress(int percentage) {
      registerProgress(percentage, null);
   }

   public void registerProgress(int percentage, String message) {
      if (_progressReporter != null) {
         _progressReporter.reportProgress(percentage, message);
      }
      _logger.log(Level.INFO, "Progress percent = "+percentage+"% msg=" +message);
   }

   public Logger getLogger() {
      return _logger;
   }
}
