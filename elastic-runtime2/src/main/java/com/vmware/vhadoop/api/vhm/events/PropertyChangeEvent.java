package com.vmware.vhadoop.api.vhm.events;

public interface PropertyChangeEvent {

	String getMoRef();
	String getPropertyKey();
	String getNewValue();
}
