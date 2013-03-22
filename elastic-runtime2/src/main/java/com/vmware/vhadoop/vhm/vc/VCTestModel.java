package com.vmware.vhadoop.vhm.vc;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.vmware.vhadoop.api.vhm.ClusterStateChangeEvent.VMEventData;
import com.vmware.vim.vmomi.client.Client;

public class VCTestModel implements com.vmware.vhadoop.api.vhm.VCActions {
   
   SyncPropertyChangeHolder _holder = new SyncPropertyChangeHolder();
   
   public class SyncPropertyChangeHolder {
      VMEventData _event;
   }
   
   @Override
   public void changeVMPowerState(String vmMoRef, boolean b) {
      System.out.println(Thread.currentThread().getName()+": VCActions: changing power state of "+vmMoRef+" to "+b+"...");
      try {
         Thread.sleep(new Random().nextInt(1000)+1000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
      simulatePowerChange(vmMoRef, b);
      System.out.println(Thread.currentThread().getName()+": VCActions: ...done changing power state of "+vmMoRef);
   }

   @Override
   public String waitForPropertyChange(String folderName, String version, List<VMEventData>  vmDataList) {
      synchronized(_holder) {
         try {
            _holder.wait();
            vmDataList.add(_holder._event);
            _holder._event = null;
            return "";
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
      return null;
   }
   
   void simulatePowerChange(String moRef, boolean isOn) {
      VMEventData vmData = new VMEventData();
      vmData._vmMoRef = moRef;
      vmData._isLeaving = false;
      vmData._powerState = isOn;
      synchronized(_holder) {
         _holder._event = vmData;
         _holder.notify();
      }
   }

   @Override
   public Client getStatsPollClient() {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public List<String> listVMsInFolder(String folderName) {
      // TODO Auto-generated method stub
      return null;
   }

}
