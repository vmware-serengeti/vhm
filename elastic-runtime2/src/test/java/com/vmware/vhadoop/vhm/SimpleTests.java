package com.vmware.vhadoop.vhm;

import org.junit.BeforeClass;
import org.junit.Test;
import java.util.*;

import com.vmware.vhadoop.api.vhm.VCActions;
import com.vmware.vhadoop.vhm.ClusterStateChangeListenerImpl.TestCluster;
import com.vmware.vhadoop.vhm.VHM;
import com.vmware.vhadoop.vhm.vc.VCTestModel;

public class SimpleTests {
   static MQClientImpl _mqClient;
   static VHM _vhm;
   static ClusterStateChangeListenerImpl _cscl;
   
   private static Set<TestCluster> getTestClusters() {
      Set<TestCluster> tc = new HashSet<TestCluster>();
      TestCluster tc1 = new TestCluster();
      tc1.addMasterVM("m1mr", "host1", "MySerengeti", "master-myCluster1", false, 0, false);
      tc1.addComputeVM("m1c1mr", "host1", "MySerengeti", "master-myCluster1", "m1mr", true);
      tc1.addComputeVM("m1c2mr", "host1", "MySerengeti", "master-myCluster1", "m1mr", true);
      tc1.addComputeVM("m1c3mr", "host1", "MySerengeti", "master-myCluster1", "m1mr", false);
      tc1.addComputeVM("m1c4mr", "host1", "MySerengeti", "master-myCluster1", "m1mr", false);
      TestCluster tc2 = new TestCluster();
      tc2.addMasterVM("m2mr", "host2", "MySerengeti", "master-myCluster2", false, 0, false);
      tc2.addComputeVM("m2c1mr", "host2", "MySerengeti", "master-myCluster2", "m2mr", true);
      tc2.addComputeVM("m2c2mr", "host2", "MySerengeti", "master-myCluster2", "m2mr", true);
      tc2.addComputeVM("m2c3mr", "host2", "MySerengeti", "master-myCluster2", "m2mr", false);
      tc2.addComputeVM("m2c4mr", "host2", "MySerengeti", "master-myCluster2", "m2mr", false);
      tc.add(tc1);
      tc.add(tc2);
      return tc;
   }
   
   private static Map<String, String> getClusterUUIDMap() {
      Map<String, String> result = new HashMap<String, String>();
      result.put("MyCluster1", "master-myCluster1");
      result.put("MyCluster2", "master-myCluster2");
      return result;
   }
   
   @BeforeClass
   public static void initMQClient() {
      VCActions vcActions = new VCTestModel();
      _vhm = new VHM(vcActions);
      _mqClient = new MQClientImpl();
      _cscl = new ClusterStateChangeListenerImpl(_vhm.getVCActions(), "MySerengeti"); //VCTestModel ignores foldername
      
      _vhm.registerEventProducer(_mqClient);
      _vhm.registerEventProducer(_cscl);
            
      _vhm.start();
      for (TestCluster tc : getTestClusters()) {
         _cscl.discoverTestCluster(tc);
      }
      _mqClient.setTestClusterUUIDMap(getClusterUUIDMap());
   }
   
   @Test
   public void testSerengetiIncrease() {
      _mqClient.doSerengetiLimitAction("MyCluster1", 4);
      _mqClient.doSerengetiLimitAction("MyCluster2", 4);
      _vhm.waitForClusterScaleCompletion();
   }

   @Test
   public void testSerengetiDecrease() {
      _mqClient.doSerengetiLimitAction("MyCluster1", 2);
      _mqClient.doSerengetiLimitAction("MyCluster2", 2);
      _vhm.waitForClusterScaleCompletion();
   }
}
