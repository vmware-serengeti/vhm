package com.vmware.vhadoop.api.vhm.strategy;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public interface ClusterScaleCompletionEventListener extends VMChooser {

   public void registerNewEvent(ClusterScaleCompletionEvent event);
   
}
