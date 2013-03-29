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

   public static final Decision ENABLE = new Decision("ENABLE");
   public static final Decision DISABLE = new Decision("DISABLE");
      
   String getClusterId();
   
   Decision getDecisionForVM(String vmId);
   
//   boolean getOutcomeCompleteForVM(String vmId);
//   
//   boolean getOutcomeCompleteForAllVMs();
}
