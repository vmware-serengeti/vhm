package com.vmware.vhadoop.api.vhm.events;

public interface ClusterScaleCompletionEvent extends NotificationEvent {

   /* No such thing as an extensible enumeration */
   public static final class Decision {
      String _value;
      public Decision(String name) {
         _value = name;
      }
      @Override
      public String toString() {
         return _value;
      }
   }

   public static final Decision EXPAND = new Decision("EXPAND");
   public static final Decision SHRINK = new Decision("SHRINK");
      
   String getClusterId();
   
   Decision getDecisionForHost(String vmId);
}
