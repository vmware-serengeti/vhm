package com.vmware.vhadoop.vhm.hadoop;

import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.vhm.model.scenarios.Serengeti.Master;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenterEntity;

public class ModelHadoopAdapter implements HadoopActions
{
   VirtualCenter vCenter;

   public ModelHadoopAdapter(VirtualCenter vCenter) {
      this.vCenter = vCenter;
   }

   @Override
   public CompoundStatus decommissionTTs(String[] taskTrackerHostnames, HadoopClusterInfo cluster) {
      return manageNodes(taskTrackerHostnames, cluster, false);
   }

   @Override
   public CompoundStatus recommissionTTs(String[] taskTrackerHostnames, HadoopClusterInfo cluster) {
      return manageNodes(taskTrackerHostnames, cluster, true);
   }

   CompoundStatus manageNodes(String[] computeNodeHostnames, HadoopClusterInfo cluster, boolean enable) {
      String action = (enable ? "Enable" : "Disable");
      CompoundStatus status = new CompoundStatus("model"+action);

      /* get the cluster Master from vCenter */

      Master master = getJobTracker(cluster, status);

      /* map the hostnames to vCenter IDs and stop the task trackers */
      for (String hostname : computeNodeHostnames) {
         CompoundStatus nodeResult = new CompoundStatus(action+" "+hostname);
         String detail;

         if (!enable) {
            detail = master.disable(hostname);
         } else {
            detail = master.enable(hostname);
         }

         if (detail == null) {
            nodeResult.registerTaskSucceeded();
         } else {
            nodeResult.registerTaskFailed(false, detail);
         }

         status.addStatus(nodeResult);
      }

      return status;
   }

   @Override
   public CompoundStatus checkTargetTTsSuccess(String opType, String[] tts, int totalTargetEnabled, HadoopClusterInfo cluster) {
      CompoundStatus status = new CompoundStatus("Check enabled task trackers");
      Master master = getJobTracker(cluster, status);

      int enabled = master.numberComputeNodesInState(true);
      if (enabled != totalTargetEnabled) {
         status.registerTaskFailed(true, "Target was "+totalTargetEnabled+", number enabled "+enabled);
      } else {
         status.registerTaskSucceeded();
      }

      return status;
   }

   private Master getJobTracker(HadoopClusterInfo cluster, CompoundStatus status) {
      String clusterId = cluster.getClusterId();
      VirtualCenterEntity vm = vCenter.get(clusterId);

      if (vm == null) {
         status.registerTaskFailed(true, "Expected the cluster ID to return the JobTracker but VirtualCenter didn't recognise the ID");
         return null;
      } else if (!(vm instanceof Master)) {
         status.registerTaskFailed(true, "Expected cluster ID ("+clusterId+") to be the ID of the JobTracker, but VirtualCenter returned: "+vm.getClass().getSimpleName());
         return null;
      }

      return (Master)vm;
   }

}
