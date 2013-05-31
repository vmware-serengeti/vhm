package com.vmware.vhadoop.vhm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vim.vmomi.client.Client;

public class StandaloneSimpleVCActions implements VCActions {
   Map<String, Object[]> _latestArgs = new HashMap<String, Object[]>();
   final PropertyChangeValues _propertyChangeValues = new PropertyChangeValues();
   boolean _isReady = false;
   Map<String, List<String>> _vmsInFolderMap = new HashMap<String, List<String>>();

   class PropertyChangeValues {
      String _returnVal;
      VMEventData _eventToReturn;
   }
   
   @Override
   public Map<String, Future<Boolean>> changeVMPowerState(Set<String> vmMoRefs,
         boolean b) {
      _latestArgs.put("changeVMPowerState", new Object[]{vmMoRefs, b});
      return null;
   }

   @Override
   public String waitForPropertyChange(String folderName, String version,
         List<VMEventData> vmDataList) throws InterruptedException {
      _latestArgs.put("waitForPropertyChange", new Object[]{folderName, version, vmDataList});
      synchronized(_propertyChangeValues) {
         _isReady = true;
         _propertyChangeValues.notify();
         _propertyChangeValues.wait();
      }
      vmDataList.add(_propertyChangeValues._eventToReturn);
      return _propertyChangeValues._returnVal;
   }
   
   Object[] getLatestMethodArgs(String methodName) {
      return _latestArgs.get(methodName);
   }

   @Override
   public void interruptWait() {
      _latestArgs.put("interruptWait", new Object[]{});
   }

   @Override
   public Client getStatsPollClient() {
      _latestArgs.put("getStatsPollClient", new Object[]{});
      return null;
   }

   @Override
   public List<String> listVMsInFolder(String folderName) {
      _latestArgs.put("listVMsInFolder", new Object[]{folderName});
      return _vmsInFolderMap.get(folderName);
   }
   
   protected void addVMToFolder(String folderName, String vmId) {
      List<String> vms = _vmsInFolderMap.get(folderName);
      if (vms == null) {
         vms = new ArrayList<String>();
         _vmsInFolderMap.put(folderName, vms);
      }
      vms.add(vmId);
   }

   public void fakeWaitForUpdatesData(String returnVal, VMEventData eventToReturn) {
      /* It's essential that we don't fake any data until a thread is actually waiting for the update */
      while (!_isReady) {
         try {
            Thread.sleep(10);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
      synchronized(_propertyChangeValues) {
         _propertyChangeValues._returnVal = returnVal;
         _propertyChangeValues._eventToReturn = eventToReturn;
         _propertyChangeValues.notify();
         try {
            _propertyChangeValues.wait();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
   }

}
