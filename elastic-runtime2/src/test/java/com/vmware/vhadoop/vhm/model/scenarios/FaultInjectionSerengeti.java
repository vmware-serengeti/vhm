package com.vmware.vhadoop.vhm.model.scenarios;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import com.vmware.vhadoop.vhm.model.api.Allocation;
import com.vmware.vhadoop.vhm.model.os.Linux;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;


public class FaultInjectionSerengeti extends Serengeti {
   private static Logger _log = Logger.getLogger(FaultInjectionSerengeti.class.getName());

   public FaultInjectionSerengeti(String id, VirtualCenter vCenter) {
      super(id, vCenter);

      /* make sure that masters are created using our enterprise template */
      masterOva = new MasterTemplate();
   }

   /**
    * Hides the template defined in Serengeti
    */
   class MasterTemplate extends Serengeti.MasterTemplate {
      @Override
      public Master create(VirtualCenter vCenter, String id, Allocation capacity) {
         Master master = new Master(vCenter, id, capacity);
         master.install(new Linux("Linux"));

         return master;
      }
   }

   public class Master extends Serengeti.Master {
      Queue<String> recommissionFailures = new LinkedList<String>();
      Queue<String> decommissionFailures = new LinkedList<String>();
      Queue<String> expectedFailureMessages = new LinkedList<String>();

      Map<String,String> expectedResponse = new HashMap<String,String>();

      Master(VirtualCenter vCenter, String id, Allocation capacity) {
         super(vCenter, id, capacity);
      }

      @Override
      public synchronized String enable(String hostname) {
         if (!recommissionFailures.isEmpty()) {
            String expectedMsg = recommissionFailures.poll();
            _log.info(name()+": injecting enable failure with reason: "+expectedMsg);
            expectedFailureMessages.add(expectedMsg);
            return expectedMsg;
         } else {
            return super.enable(hostname);
         }
      }

      @Override
      public synchronized String disable(String hostname) {
         if (!decommissionFailures.isEmpty()) {
            String expectedMsg = decommissionFailures.poll();
            _log.info(name()+": injecting disable failure with reason: "+expectedMsg);
            expectedFailureMessages.add(expectedMsg);
            return expectedMsg;
         } else {
            return super.disable(hostname);
         }
      }

      public synchronized void queueRecommissionFailure(String errorMessage) {
         recommissionFailures.add(errorMessage);
      }

      public synchronized void queueDecommissionFailure(String errorMessage) {
         decommissionFailures.add(errorMessage);
      }

      @Override
      public synchronized void deliverMessage(String msgId, VHMJsonReturnMessage msg) {
         _log.info(name()+": received message, id: "+msgId+
               ", finished: "+msg.finished+
               ", succeeded: "+msg.succeed+
               ", progress: "+msg.progress+
               ", error_code: "+msg.error_code+
               ", error_msg: "+msg.error_msg+
               ", progress_msg: "+msg.progress_msg);

         if (msg.finished && !msg.succeed && !expectedFailureMessages.isEmpty()) {
            synchronized(expectedResponse) {
               expectedResponse.put(expectedFailureMessages.poll(), msg.error_msg);
               expectedResponse.notifyAll();
            }
         }
      }

      /**
       * This returns a map of the error message that was last popped from the queued failures and the
       * error message that is returned via Rabbit
       * @return
       */
      public Map<String,String> getResponses(long timeout) {
         synchronized(expectedResponse) {
            try {
               long deadline = System.currentTimeMillis() + timeout;
               long remaining = timeout;
               while (expectedResponse.isEmpty() && (remaining = deadline - System.currentTimeMillis()) > 0) {
                  expectedResponse.wait(remaining);
               }
            } catch (InterruptedException e) {}

            Map<String,String> results = new HashMap<String,String>(expectedResponse);
            expectedResponse.clear();

            return results;
         }
      }
   }
}
