package com.vmware.vhadoop.vhm.events;

import java.util.Collections;

import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public class TrivialClusterScaleCompletionEvent extends ClusterScaleDecision {
   /* A lot of the testing depends on differentiating between one event and another
    * Since timestamp is not part of the equals method, each returned event is given a unique ID */
   public static int _idCntr = 0;
   public int _id = _idCntr++;
   
   public TrivialClusterScaleCompletionEvent(String clusterId) {
      super(clusterId);
   }
   
   @Override
   public ClusterScaleCompletionEvent immutableCopy() {
      TrivialClusterScaleCompletionEvent copy = new TrivialClusterScaleCompletionEvent(_clusterId);
      copy._decisions = (_decisions == null) ? null : Collections.unmodifiableMap(_decisions);
      copy._eventsToRequeue = (_eventsToRequeue == null) ? null : Collections.unmodifiableList(_eventsToRequeue);
      copy._id = _id;
      return copy;
   }

   @Override
   public boolean equals(Object o) {
      if (super.equals(o)) {
         return ((TrivialClusterScaleCompletionEvent)o)._id == _id;
      }
      return false;
   }
   
   @Override
   public int hashCode() {
      return super.hashCode()+_id;
   }

}
