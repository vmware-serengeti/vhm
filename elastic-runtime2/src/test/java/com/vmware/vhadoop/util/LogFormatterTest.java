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

package com.vmware.vhadoop.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/* Formatter substitution for VM and Cluster Ids:
 * A VM ID should be wrapped in <%V %V>, unless it is the last part of the String, in which case <%V will suffice
 * A Cluster ID should be wrapped in <%C %C>, unless it is the last part of the String, in which case <%C will suffice */

public class LogFormatterTest {
   private final String vmId1 = "vm-1";
   private final String vmId2 = "vm-2";
   private final String clusterId1 = "cluster-1";
   private final String clusterId2 = "cluster-2";

   private final String vmName1 = "vmName1";
   private final String vmName2 = "vmName2";
   private final String clusterName1 = "clusterName1";
   private final String clusterName2 = "clusterName2";

   private final String testString1 = "hello";
   private final String testString2 = "foo <%V" + vmId1 + "%V> bar";
   private final String testString3 = "foo <%V" + vmId1 + "%V> bar <%V" + vmId2 + "%V> doo";
   private final String testString4 = "foo <%C" + clusterId1 + "%C> bar";
   private final String testString5 = "foo <%C" + clusterId1 + "%C> bar <%C" + clusterId2 + "%C> doo";
   private final String testString2a = "<%V" + vmId1 + "%V> bar";
   private final String testString3a = "<%V" + vmId1 + "%V> bar <%V" + vmId2 + "%V> doo";
   private final String testString4a = "<%C" + clusterId1 + "%C> bar";
   private final String testString5a = "<%C" + clusterId1 + "%C> bar <%C" + clusterId2 + "%C> doo";
   private final String testString2b = "foo <%V" + vmId1 + "%V>";
   private final String testString3b = "foo <%V" + vmId1 + "%V> bar <%V" + vmId2 + "%V>";
   private final String testString4b = "foo <%C" + clusterId1 + "%C>";
   private final String testString5b = "foo <%C" + clusterId1 + "%C> bar <%C" + clusterId2 + "%C>";
   private final String testString2c = "<%V" + vmId1 + "%V>";
   private final String testString3c = "<%V" + vmId1 + "%V><%V" + vmId2 + "%V>";
   private final String testString4c = "<%C" + clusterId1 + "%C>";
   private final String testString5c = "<%C" + clusterId1 + "%C><%C" + clusterId2 + "%C>";
   private final String combo1 = testString1 + testString3 + testString4 + testString2 + testString5;
   private final String wrongVM = "foo <%V" + "vm-5" + "%V> bar";
   private final String wrongCluster = "foo <%C" + "cluster-5" + "%C> bar";
   private final String missingTerminatorVM = "foo <%V" + vmId1 + "%V bar";
   private final String missingTerminatorCluster = "foo <%C" + clusterId1 + "%> bar";
   private final String missingCombo1 = testString4 + missingTerminatorVM;
   private final String missingCombo2 = missingTerminatorVM + testString2;
   private final String missingCombo3 = testString4 + missingTerminatorVM + testString2;
   private final String missingCombo4 = testString2 + missingTerminatorVM + testString4;
   private final String missingCombo5 = testString2 + missingTerminatorVM;
   private final String missingCombo6 = missingTerminatorVM + testString4;
   private final String okMissingTerminator1 =  "foo <%V" + vmId1;
   private final String okMissingTerminator2 =  "foo <%C" + clusterId1;
   private final String okMissingTerminator3 =  "foo <%V" + vmId1 + "\n";
   private final String okMissingTerminator4 =  "foo <%C" + clusterId1+"\n";

   private final String testString1Ex = testString1;
   private final String testString2Ex = "foo " + vmName1 + " bar";
   private final String testString3Ex = "foo " + vmName1 + " bar " + vmName2 + " doo";
   private final String testString4Ex = "foo " + clusterName1 + " bar";
   private final String testString5Ex = "foo " + clusterName1 + " bar " + clusterName2 + " doo";
   private final String testString2aEx = vmName1 + " bar";
   private final String testString3aEx = vmName1 + " bar " + vmName2 + " doo";
   private final String testString4aEx = clusterName1 + " bar";
   private final String testString5aEx = clusterName1 + " bar " + clusterName2 + " doo";
   private final String testString2bEx = "foo " + vmName1;
   private final String testString3bEx = "foo " + vmName1 + " bar " + vmName2;
   private final String testString4bEx = "foo " + clusterName1;
   private final String testString5bEx = "foo " + clusterName1 + " bar " + clusterName2;
   private final String testString2cEx = vmName1;
   private final String testString3cEx = vmName1 + vmName2;
   private final String testString4cEx = clusterName1;
   private final String testString5cEx = clusterName1 + clusterName2;
   private final String combo1Ex = testString1Ex + testString3Ex + testString4Ex + testString2Ex + testString5Ex;
   private final String wrongVMEx = "foo " + "vm-5" + " bar";
   private final String wrongClusterEx = "foo " + "cluster-5" + " bar";
   private final String missingTerminatorVMEx = missingTerminatorVM;
   private final String missingTerminatorClusterEx = missingTerminatorCluster;
   private final String missingCombo1Ex = testString4Ex + missingTerminatorVMEx;
   private final String missingCombo2Ex = missingTerminatorVMEx + testString2Ex;
   private final String missingCombo3Ex = testString4Ex + missingTerminatorVMEx + testString2Ex;
   private final String missingCombo4Ex = testString2Ex + missingTerminatorVMEx + testString4Ex;
   private final String missingCombo5Ex = testString2Ex + missingTerminatorVMEx;
   private final String missingCombo6Ex = missingTerminatorVMEx + testString4Ex;
   private final String okMissingTerminator1Ex =  "foo " + vmName1;
   private final String okMissingTerminator2Ex =  "foo " + clusterName1;
   private final String okMissingTerminator3Ex =  "foo " + vmName1 + "\n";
   private final String okMissingTerminator4Ex =  "foo " + clusterName1+"\n";

   Map<String, String> testData;
   
   @Before
   public void init() {
      LogFormatter._vmIdToNameMapper.put(vmId1, vmName1);
      LogFormatter._vmIdToNameMapper.put(vmId2, vmName2);
      LogFormatter._clusterIdToNameMapper.put(clusterId1, clusterName1);
      LogFormatter._clusterIdToNameMapper.put(clusterId2, clusterName2);
      
      testData = new HashMap<String, String>();
      
      testData.put(testString1, testString1Ex);
      testData.put(testString2, testString2Ex);
      testData.put(testString3, testString3Ex);
      testData.put(testString4, testString4Ex);
      testData.put(testString5, testString5Ex);
      testData.put(testString2a, testString2aEx);
      testData.put(testString3a, testString3aEx);
      testData.put(testString4a, testString4aEx);
      testData.put(testString5a, testString5aEx);
      testData.put(testString2b, testString2bEx);
      testData.put(testString3b, testString3bEx);
      testData.put(testString4b, testString4bEx);
      testData.put(testString5b, testString5bEx);
      testData.put(testString2c, testString2cEx);
      testData.put(testString3c, testString3cEx);
      testData.put(testString4c, testString4cEx);
      testData.put(testString5c, testString5cEx);
      testData.put(combo1, combo1Ex);
      testData.put(wrongVM, wrongVMEx);
      testData.put(wrongCluster, wrongClusterEx);
      testData.put(missingTerminatorVM, missingTerminatorVMEx);
      testData.put(missingTerminatorCluster, missingTerminatorClusterEx);
      testData.put(missingCombo1, missingCombo1Ex);
      testData.put(missingCombo2, missingCombo2Ex);
      testData.put(missingCombo3, missingCombo3Ex);
      testData.put(missingCombo4, missingCombo4Ex);
      testData.put(missingCombo5, missingCombo5Ex);
      testData.put(missingCombo6, missingCombo6Ex);
      testData.put(okMissingTerminator1, okMissingTerminator1Ex);
      testData.put(okMissingTerminator2, okMissingTerminator2Ex);
      testData.put(okMissingTerminator3, okMissingTerminator3Ex);
      testData.put(okMissingTerminator4, okMissingTerminator4Ex);
   }
   
   @Test
   public void substitutionTests() {
      for (String key : testData.keySet()) {
         String value = testData.get(key);
         String actual = LogFormatter.swapIdsForNames(new StringBuilder(value)).toString();
//         System.out.println("TESTING \""+key+"\"; EXPECTING \""+value+"\"; GOT \""+actual+"\"");
         assertEquals(value, actual);
      }
   }
   
   @Test
   public void collectionFormatting() {
      String result = LogFormatter.constructListOfLoggableVms(new HashSet(Arrays.asList(new String[]{vmId1, vmId2})));
      result = LogFormatter.swapIdsForNames(new StringBuilder(result)).toString();
      assertEquals(vmName1+", "+vmName2, result);
      
      result = LogFormatter.constructListOfLoggableVms(new HashSet(Arrays.asList(new String[]{vmId1})));
      result = LogFormatter.swapIdsForNames(new StringBuilder(result)).toString();
      assertEquals(vmName1, result);
      
      result = LogFormatter.constructListOfLoggableVms(new HashSet(Arrays.asList(new String[]{})));
      result = LogFormatter.swapIdsForNames(new StringBuilder(result)).toString();
      assertEquals("[]", result);
      
      result = LogFormatter.constructListOfLoggableVms(null);
      result = LogFormatter.swapIdsForNames(new StringBuilder(result)).toString();
      assertEquals("null", result);
   }
}
