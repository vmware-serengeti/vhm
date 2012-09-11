package com.vmware.vhadoop.external;

import java.util.HashMap;
import java.util.Map;

public class OVF {

   Map<String, String> _vmxData;
   String _className;
   long _vRAM;

   public OVF(String className, long vRAM) {
      _className = className;
      _vRAM = vRAM;
   }

   public String getClassName() {
      return _className;
   }

   public long getVRAM() {
      return _vRAM;
   }

   public void addVmxData(String key, String value) {
      if (_vmxData == null) {
         _vmxData = new HashMap<String, String>();
      }
      _vmxData.put(key, value);
   }

   public String getVmxData(String key, String value) {
      if (_vmxData == null) {
         return null;
      }
      return _vmxData.get(key);
   }
}
