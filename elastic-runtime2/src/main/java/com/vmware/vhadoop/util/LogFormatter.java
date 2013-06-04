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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IllegalFormatConversionException;
import java.util.Map;
import java.util.UnknownFormatConversionException;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter {
   public static Map<String, String> _vmIdToNameMapper = Collections.synchronizedMap(new HashMap<String, String>());
   public static Map<String, String> _clusterIdToNameMapper = Collections.synchronizedMap(new HashMap<String, String>());

   public static String VMID_PREFIX = "<%V";
   public static String VMID_POSTFIX = "%V>";
   public static String CLUSTERID_PREFIX = "<%C";
   public static String CLUSTERID_POSTFIX = "%C>";

   public static final String NEWLINE = System.getProperty("line.separator");
   private static final int NEWLINE_LENGTH = NEWLINE == null ? 0 : NEWLINE.length();

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

      result.append(" [").append(Thread.currentThread().getName()).append("-").append(name);
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
      if (record.getThrown() != null) {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         PrintWriter pw = new PrintWriter(baos);
         record.getThrown().printStackTrace(pw);
         pw.close();
         result.append(baos.toString());
         result.append(NEWLINE);
      }
      try {
         result = swapIdsForNames(result);
      } catch (Exception e) {
         result.insert(0, "FOUND BADLY FORMATTED LOG MSG: ");
      }
      return result.toString();
   }

   private static class IntWrapper {
      int _nextKey;
   }

   private static int substituteIdForName(StringBuilder hasIds, StringBuilder result, Map<String, String> mapper,
         IntWrapper state, String prefixStr, String postfixStr, int nextInbetweenText) {
      int postfixLength = postfixStr.length();
      int prefixLength = prefixStr.length();
      int hasIdsLength = hasIds.length();
      int startCurrentKey = state._nextKey;
      int endCurrentKey = hasIds.indexOf(postfixStr, startCurrentKey);
      int newInbetweenText = nextInbetweenText;

      /* If the current key has no postfix, it is either the end of the string or is badly formatted */
      if (endCurrentKey < 0) {
         int nextSpace = hasIds.indexOf(" ", state._nextKey);     /* Is it the end of the String? */
         postfixLength = 0;
         if (nextSpace < 0) {                                     /* Yes, it is the end of the String */
            endCurrentKey = hasIdsLength;
            if (hasIds.substring(hasIdsLength - NEWLINE_LENGTH).equals(NEWLINE)) {
               endCurrentKey-= NEWLINE_LENGTH;
            }

//            if (hasIds.charAt(hasIdsLength - NEWLINE_LENGTH) == '\n') {
//               endCurrentKey--;                                   /* Ensure that the '\n' gets added back in at the end */
//            }
         }
      }

      /* If this is well formatted */
      if (endCurrentKey > 0) {
         String id = hasIds.substring(startCurrentKey + prefixLength, endCurrentKey);
         String name = mapper.get(id);
         result.append(hasIds.substring(nextInbetweenText, state._nextKey));  /* Append the bit before the substitution */
         result.append((name == null) ? id : name);                     /* Append the substitution */
         state._nextKey = endCurrentKey;
         newInbetweenText = endCurrentKey + postfixLength;
      } else {
         /* Badly formatted - skip on */
         state._nextKey = state._nextKey + prefixLength;
      }

      /* Return the index of the text immediately after the substitution */
      return newInbetweenText;
   }

   /* Formatter substitution for VM and Cluster Ids:
    * A VM ID should be wrapped in <%V %V>, unless it is the last part of the String, in which case <%V will suffice
    * A Cluster ID should be wrapped in <%C %C>, unless it is the last part of the String, in which case <%C will suffice
    * Eg. "This is a vm <%V"+vmid+"%V> that I'm printing"
    * Eg. "This is the last word on vm <%V"+vmId
    * If a VM or ClusterId is unrecognized, the formatting is stripped out and the Id is used
    */
   static StringBuilder swapIdsForNames(StringBuilder hasIds) {
      StringBuilder result = hasIds;
      boolean hasVMKey = false;
      boolean hasClusterKey = false;
      IntWrapper vmKeyState = new IntWrapper();
      IntWrapper clusterKeyState = new IntWrapper();
      int nextInbetweenText = -1;
      do {
         if (vmKeyState._nextKey >= 0) {
            vmKeyState._nextKey = hasIds.indexOf(VMID_PREFIX, vmKeyState._nextKey);
         }
         if (clusterKeyState._nextKey >= 0) {
            clusterKeyState._nextKey = hasIds.indexOf(CLUSTERID_PREFIX, clusterKeyState._nextKey);
         }
         hasVMKey = vmKeyState._nextKey >= 0;
         hasClusterKey = clusterKeyState._nextKey >= 0;
         if (hasVMKey || hasClusterKey) {
            if (result == hasIds) {
               /* We're going to transform the string, so don't return the original, build a new one */
               result = new StringBuilder();
            }
            if (nextInbetweenText == -1) {
               nextInbetweenText = 0;
            }
            if (hasVMKey && (!hasClusterKey || (vmKeyState._nextKey < clusterKeyState._nextKey))) {
               nextInbetweenText = substituteIdForName(hasIds, result, _vmIdToNameMapper, vmKeyState,
                     VMID_PREFIX, VMID_POSTFIX, nextInbetweenText);
            } else
            if (hasClusterKey && (!hasVMKey || (clusterKeyState._nextKey < vmKeyState._nextKey))) {
               nextInbetweenText = substituteIdForName(hasIds, result, _clusterIdToNameMapper, clusterKeyState,
                     CLUSTERID_PREFIX, CLUSTERID_POSTFIX, nextInbetweenText);
            }
         }
      } while (hasVMKey || hasClusterKey);
      if (result != hasIds) {
         result.append(hasIds.substring(nextInbetweenText));
      }
      return result;
   }

}
