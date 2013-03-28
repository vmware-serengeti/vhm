package com.vmware.vhadoop.vhm.events;

import java.util.logging.Logger;

import com.vmware.vhadoop.vhm.rabbit.RabbitAdaptor.RabbitConnectionCallback;
import com.vmware.vhadoop.vhm.rabbit.VHMJsonReturnMessage;

public class SerengetiLimitInstruction extends AbstractClusterScaleEvent {
   String _clusterFolderName;
   int _toSize;
   RabbitConnectionCallback _messageCallback;

   private static final Logger _log = Logger.getLogger(SerengetiLimitInstruction.class.getName());

   public SerengetiLimitInstruction(String clusterFolderName, int toSize, RabbitConnectionCallback messageCallback) {
      _clusterFolderName = clusterFolderName;
      _toSize = toSize;
      _messageCallback = messageCallback;
   }

   public String getClusterFolderName() {
      return _clusterFolderName;
   }
   
   public int getToSize() {
      return _toSize;
   }
   
   public void reportProgress(int percentage, String message) {
      if (_messageCallback != null) {
         _log.info("Reporting progress "+percentage+"%");
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(false, false, percentage, 0, null, message);
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   public void reportError(String message) {
      if (_messageCallback != null) {
         _log.info("Reporting error "+message);
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, false, 100, 0, message, null);
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }

   public void reportCompletion() {
      if (_messageCallback != null) {
         _log.info("Reporting completion");
         VHMJsonReturnMessage msg = new VHMJsonReturnMessage(true, true, 100, 0, null, null);
         _messageCallback.sendMessage(msg.getRawPayload());
      }
   }
}
