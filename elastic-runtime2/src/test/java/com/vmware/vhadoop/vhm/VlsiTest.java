/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

package com.vmware.vhadoop.vhm;

import java.util.List;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.api.vhm.VCActions.VMEventData;

public class VlsiTest {

   @Ignore // disabled as this requires a configured VC connection with a live VC at the other end
   @Test
   public void test() {
      BootstrapMain mc = new BootstrapMain();
      VCActions vcActions = mc.getVCInterface(null);
      Properties properties = mc.getProperties();
      String version = "";
      while (true) {
         List<VMEventData> vmDataList = null;
         try {
            vmDataList = vcActions.waitForPropertyChange(properties.getProperty("uuid"));
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
         if (vmDataList != null) {
            for (VMEventData vmData : vmDataList) {
               System.out.println(Thread.currentThread().getName()+": ClusterStateChangeListener: detected change moRef= "
                     +vmData._vmMoRef + " leaving=" + vmData._isLeaving);
            }
         }
      }
   }


}
