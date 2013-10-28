/**
 * Copyright \u00A9 2013 VMware, Inc. All rights reserved. This product is protected by copyright and
 * intellectual property laws in the United States and other countries as well as by international treaties.
 * VMware products are covered by one or more patents listed at http://www.vmware.com/go/patents.
 */
package com.vmware.vhadoop.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.vmware.vhadoop.vhm.BootstrapMain;

public class ExternalizedParameters
{
   protected static final Map<String,ExternalizedParameters> handles = new HashMap<String,ExternalizedParameters>();
   private static final String baseName = "externalized_parameters.properties";

   protected Properties properties;
   private final Properties baseProperties;
   private final String filename;

   public synchronized static ExternalizedParameters get() {
      return get("");
   }

   public synchronized static ExternalizedParameters get(String label) {
      ExternalizedParameters params = handles.get(label);
      if (params == null) {
         params = new ExternalizedParameters(label);
         handles.put(label, params);
      }

      return params;
   }

   private ExternalizedParameters(String label) {
      String propertiesFile = baseName;
      if (label.length() > 0) {
         filename = propertiesFile.replace(".properties", "_"+label+".properties");
      } else {
         filename = baseName;
      }

      properties = BootstrapMain.readPropertiesFile(filename);
      baseProperties = properties;
   }

   public String getProperty(final String key) {
      if (properties == null) {
         return null;
      }

      return properties.getProperty(key);
   }

   public void setOverlay(Properties overlay) {
      Properties merged = new Properties(properties);
      merged.putAll(overlay);
      properties = merged;
   }

   public Set<String> getPropertyNames() {
      return properties.stringPropertyNames();
   }

   public void clearOverlays() {
      properties = baseProperties;
   }

   public int getInt(final String key) {
      return Integer.valueOf(getProperty(key));
   }

   public long getLong(final String key) {
      return Long.valueOf(getProperty(key));
   }

   public float getFloat(final String key) {
      return Float.valueOf(getProperty(key));
   }

   public double getDouble(final String key) {
      return Double.valueOf(getProperty(key));
   }

   public String getString(final String key) {
      return getProperty(key);
   }

   public String getFilename() {
      return filename;
   }
}
