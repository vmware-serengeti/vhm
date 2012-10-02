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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.IllegalFormatConversionException;
import java.util.UnknownFormatConversionException;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter {

   public static final String NEWLINE = System.getProperty("line.separator");

   @Override
   public String format(LogRecord record) {
      String name = record.getLoggerName();
      if (name == null) {
         name = "root";
      } else {
         int ix = name.lastIndexOf('.');
         if (ix != -1) {
            name = name.substring(ix + 1);
         }
      }
      StringBuilder result = new StringBuilder();

      // timestamp prefix (e.g. 2012 Sep 17 17:20:20.852)
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd HH:mm:ss.S");
      result.append(sdf.format(new Date()));

      result.append(" [").append(record.getThreadID()).append("-").append(name);
      /* Fine logging is for method entry/exit so we add the method name */
      if (record.getLevel().equals(Level.FINE) || record.getLevel().equals(Level.FINER)
            || record.getLevel().equals(Level.FINEST)) {
         result.append(".").append(record.getSourceMethodName());
      }
      result.append("] ");
      Object[] params = record.getParameters();
      String rawMessage = null;
      if ((params != null) && (params.length > 0)) {
         try {
            rawMessage = String.format(record.getMessage(), params);
            result.append(rawMessage);
         } catch (IllegalFormatConversionException e) {
         } catch (UnknownFormatConversionException e) {
            result.append("FOUND BADLY FORMATTED LOG MSG: ").append(record.getMessage());
         }
      } else {
         result.append((rawMessage = record.getMessage()));
      }
      result.append(NEWLINE);
      return result.toString();
   }

}
