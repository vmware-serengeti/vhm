package com.vmware.vhadoop.vhm;

import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.events.ClusterStateChangeEvent.VMEventData;

public class VlsiTest {

   @Test
   public void test() {
      BootstrapMain mc = new BootstrapMain();
      VCActions vcActions = mc.getVCInterface();
      Properties properties = mc.getProperties();
      String version = "";
      while (true) {
         ArrayList<VMEventData> vmDataList = new ArrayList<VMEventData>(); 
         version = vcActions.waitForPropertyChange(properties.getProperty("uuid"), version, vmDataList);
         for (VMEventData vmData : vmDataList) {
            System.out.println(Thread.currentThread().getName()+": ClusterStateChangeListener: detected change moRef= "
                  +vmData._vmMoRef + " leaving=" + vmData._isLeaving);
         }
      }
   }

}
