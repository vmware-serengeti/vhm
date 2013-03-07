package com.vmware.vhadoop.vhm;

import java.util.Random;

public class VCTestModel implements com.vmware.vhadoop.api.vhm.VCActions {
   
   SyncPropertyChangeHolder _holder = new SyncPropertyChangeHolder();
   
   public static final String PROPERTY_KEY_POWER_STATE = "powerState";

   public class SyncPropertyChangeHolder {
      PropertyChangeEvent _event;
   }
   
   public class PropertyChangeEvent implements com.vmware.vhadoop.api.vhm.events.PropertyChangeEvent {
      public PropertyChangeEvent(String moRef, String propertyKey, String newValue) {
         _moRef = moRef; 
         _propertyKey = propertyKey;
         _newValue = newValue;
      }
      String _moRef;
      String _propertyKey;
      String _newValue;

      @Override
      public String getMoRef() {
         return _moRef;
      }
      @Override
      public String getPropertyKey() {
         return _propertyKey;
      }
      @Override
      public String getNewValue() {
         return _newValue;
      }
   }
   
   @Override
   public void changeVMPowerState(String vmMoRef, boolean b) {
      System.out.println(Thread.currentThread().getName()+": VCActions: changing power state of "+vmMoRef+" to "+b+"...");
      try {
         Thread.sleep(new Random().nextInt(1000)+1000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      simulatePropertyChange(vmMoRef, PROPERTY_KEY_POWER_STATE, (b ? "true" : "false"));
      System.out.println(Thread.currentThread().getName()+": VCActions: ...done changing power state of "+vmMoRef);
   }

   @Override
   public com.vmware.vhadoop.api.vhm.events.PropertyChangeEvent waitForPropertyChange() {
      synchronized(_holder) {
         try {
            _holder.wait();
            PropertyChangeEvent event = _holder._event;
            _holder._event = null;
            return event;
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
      return null;
   }
   
   void simulatePropertyChange(String moRef, String key, String value) {
      synchronized(_holder) {
         _holder._event = new PropertyChangeEvent(moRef, key, value);
         _holder.notify();
      }
   }
}
