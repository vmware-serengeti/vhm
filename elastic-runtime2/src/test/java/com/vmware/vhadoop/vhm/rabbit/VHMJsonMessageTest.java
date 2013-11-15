package com.vmware.vhadoop.vhm.rabbit;

import org.junit.Test;
import static org.junit.Assert.*;

import com.vmware.vhadoop.vhm.events.SerengetiLimitInstruction.SerengetiLimitAction;

public class VHMJsonMessageTest {

   private byte[] getRawData(String action, String clusterId, String instanceNum, String routeKey, String version) {
      String rawJson = "{"+
        "\"cluster_name\": \""+clusterId+"\","+
        "\"route_key\": \""+routeKey+"\","+
        "\"action\": \""+action+"\","+
        "\"instance_num\": \""+instanceNum+"\","+
        "\"version\": \""+version+"\""+
       "}";
      return rawJson.getBytes();
   }
   
   @Test
   public void testParsing() {
      SerengetiLimitAction inputAction = SerengetiLimitAction.actionSetTarget;
      String clusterId = "cluster";
      String instanceNum = "4";
      String routeKey = "routeKey";
      String version = "3";
      VHMJsonInputMessage toTest = new VHMJsonInputMessage(getRawData(inputAction.toString(), clusterId, instanceNum, routeKey, version));

      SerengetiLimitAction outputAction = toTest.getAction();
      assertTrue(outputAction.equals(inputAction));
      assertEquals(outputAction.toString(), inputAction.toString());
      
      assertEquals(clusterId, toTest.getClusterId());
      assertEquals(Integer.parseInt(instanceNum), toTest.getInstanceNum());
      assertEquals(routeKey, toTest.getRouteKey());
   }
   
   private void assertEmptyResult(VHMJsonInputMessage toTest) {
      assertNull(toTest.getAction());
      assertNull(toTest.getClusterId());
      assertEquals(toTest.getInstanceNum(), 0);
      assertNull(toTest.getRouteKey());
   }
   
   @Test
   public void unknownVersion() {
      SerengetiLimitAction inputAction = SerengetiLimitAction.actionSetTarget;
      String clusterId = "cluster";
      String instanceNum = "4";
      String routeKey = "routeKey";
      String version = "2";
      VHMJsonInputMessage toTest = new VHMJsonInputMessage(getRawData(inputAction.toString(), clusterId, instanceNum, routeKey, version));
      assertEmptyResult(toTest);
   }
   
   @Test
   public void unknownAction() {
      String clusterId = "cluster";
      String instanceNum = "4";
      String routeKey = "routeKey";
      String version = "3";
      VHMJsonInputMessage toTest = new VHMJsonInputMessage(getRawData("invalid", clusterId, instanceNum, routeKey, version));
      assertEmptyResult(toTest);
   }

   @Test
   public void emptyInput() {
      VHMJsonInputMessage toTest = new VHMJsonInputMessage(new byte[0]);
      assertEmptyResult(toTest);
   }

   @Test
   public void unparsableInput() {
      VHMJsonInputMessage toTest = new VHMJsonInputMessage(new byte[]{1,2,3,4,5,6});
      assertEmptyResult(toTest);
   }
}
