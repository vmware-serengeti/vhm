package com.vmware.vhadoop.api.vhm;

import java.util.List;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.events.ClusterHealthEvent;
import com.vmware.vhadoop.api.vhm.events.NotificationEvent;
import com.vmware.vhadoop.api.vhm.strategy.VMChooser;

public interface HealthManager extends VMChooser {
   
   public interface HealthMonitor {

      /* TODO: Should this return anything? */
      void handleHealthEvents(VCActions _vcActions, Set<ClusterHealthEvent> healthEvents);

   }

   HealthMonitor getHealthMonitor();

   /* If multiple events are added at once, we should be able to scan them all at once */
   Set<ClusterHealthEvent> checkHealth(List<? extends NotificationEvent> events);

   Set<ClusterHealthEvent> checkHealth(NotificationEvent event);

}
