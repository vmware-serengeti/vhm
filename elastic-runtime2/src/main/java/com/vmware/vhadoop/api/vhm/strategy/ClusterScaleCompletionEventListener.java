package com.vmware.vhadoop.api.vhm.strategy;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

/* Could be used to listen to scale decisions on hosts for balancing purposes */
public interface ClusterScaleCompletionEventListener extends VMChooser {

   public void registerNewEvent(ClusterScaleCompletionEvent event);
   
}
