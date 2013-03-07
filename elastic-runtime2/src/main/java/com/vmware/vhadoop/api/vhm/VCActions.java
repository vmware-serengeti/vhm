package com.vmware.vhadoop.api.vhm;

import com.vmware.vhadoop.api.vhm.events.PropertyChangeEvent;

public interface VCActions {

   public void changeVMPowerState(String vmMoRef, boolean b);

   public PropertyChangeEvent waitForPropertyChange();

}
