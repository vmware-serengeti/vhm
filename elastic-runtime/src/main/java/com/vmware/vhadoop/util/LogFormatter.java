package com.vmware.vhadoop.util;

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
