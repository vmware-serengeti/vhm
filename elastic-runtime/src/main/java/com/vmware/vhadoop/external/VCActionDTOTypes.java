package com.vmware.vhadoop.external;

import java.util.HashMap;
import java.util.Map;

public abstract class VCActionDTOTypes {
   public enum VMPowerState {
      POWERED_ON, POWERED_OFF, UNKNOWN;
   }

   public static class VCDTO {
      public String _name;
      public Object _moId;
      private Map<String, Object> _currentStateProperties;

      public VCDTO(String name) {
         _name = name;
      }

      @Override
      public String toString() {
         return _name;
      }
      
      /* Current state properties define what the state of the properties were when the query was made */
      public void addStateProperty(String key, Object value) {
         if (_currentStateProperties == null) {
            _currentStateProperties = new HashMap<String, Object>();
         }
         _currentStateProperties.put(key, value);
      }
      
      public void setCurrentStateProperties(Map<String, Object> stateProps) {
         _currentStateProperties = stateProps;
      }
      
      public Object getStateProperty(String key) {
         if (_currentStateProperties != null) {
            return _currentStateProperties.get(key);
         }
         return null;
      }
   }

   /*
    * Constructors don't have moId since this is data which can only be set by VC for returned data - VC sets it directly
    * Also, constructors should all just take a name so that we can create them generically
    */
   public static class ResourcePoolDTO extends VCDTO {
      public long _memoryLimit;

      public ResourcePoolDTO(String name) {
         super(name);
      }

      void setMemoryLimit(long limit) {
         _memoryLimit = limit;
      }
   }

   public static class HostDTO extends VCDTO {
      public HostDTO(String name) {
         super(name);
      }
   }

   public static class VMDTO extends VCDTO {
      public VMDTO(String name) {
         super(name);
      }
   }

   public static class ClusterDTO extends VCDTO {
      public ClusterDTO(String name) {
         super(name);
      }
   }

   public static class DataCenterDTO extends VCDTO {
      public DataCenterDTO(String name) {
         super(name);
      }
   }

   public static class FolderDTO extends VCDTO {
      public FolderDTO(String name) {
         super(name);
      }
   }
}
