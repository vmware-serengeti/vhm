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

package com.vmware.vhadoop.vhm.hadoop;

import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_COMMAND_NOT_FOUND;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_DRLIST_FILE_NOT_FOUND;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_EXCESS_TTS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_FEWER_TTS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_JT_CONNECTION;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.SUCCESS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.UNKNOWN_ERROR;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.WARN_TT_ACTIVE;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.WARN_TT_EXCLUDESFILE;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.vhm.model.hadoop.JobTracker;
import com.vmware.vhadoop.vhm.model.hadoop.TaskTracker;
import com.vmware.vhadoop.vhm.model.scenarios.Master;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenterEntity;

public class ModelHadoopConnection extends HadoopConnection
{
   VirtualCenter vCenter;
   JobTracker jobTracker;
   Map<String,String> files = new HashMap<String,String>();

   public ModelHadoopConnection(VirtualCenter vCenter, HadoopClusterInfo cluster, HadoopConnectionProperties props) {
      super(cluster, props, null);
      this.vCenter = vCenter;

      jobTracker = getJobTracker(cluster);
   }

   protected JobTracker getJobTracker(HadoopClusterInfo cluster) {
      String clusterId = cluster.getClusterId();
      VirtualCenterEntity vm = vCenter.get(clusterId);

      if (vm == null) {
         return null;
      } else if (!(vm instanceof Master)) {
         return null;
      }

      Master master = (Master)vm;

      String port = master.getExtraInfo().get("vhmInfo.jobtracker.port");
      return (JobTracker)master.getOS().connect(port);
   }

   /**
    * This is a no-op from the point of view of basic glue into the model. We could add additional fault injection capabilities here.
    */
   @Override
   public int copyDataToJobTracker(byte[] inputData, String remotePath, String remoteFileName, boolean isExecutable) {
      if (jobTracker == null) {
         return ERROR_JT_CONNECTION;
      }

      /* store the file for later reference */
      /* TODO: see if we want to stash this on the actual job tracker VM as a "file" rather than just noting it here in the connection */
      files.put(remotePath+remoteFileName, new String(inputData));

      return SUCCESS;
   }

   @Override
   public int executeScript(String scriptFileName, String destinationPath, String[] args, OutputStream out) {
      if (!files.containsKey(destinationPath + scriptFileName)) {
         return ERROR_COMMAND_NOT_FOUND;
      }

      try {
         if (scriptFileName.equals("checkTargetTTsSuccess.sh")) {
            return shCheckTargetTTsSuccess(args, out);
         } else if (scriptFileName.equals("decommissionTTs.sh")) {
            return shUpdateTTs(args, out, false);
         } else if (scriptFileName.equals("recommissionTTs.sh")) {
            return shUpdateTTs(args, out, true);
         }

         return ERROR_COMMAND_NOT_FOUND;
      } catch (IOException e) {
         return UNKNOWN_ERROR;
      }
   }

   /**
    *
    * @param args totalTargetEnabled, excludesFilePath, hadoopHome
    * @param out
    * @return
    * @throws IOException
    */
   protected int shCheckTargetTTsSuccess(String args[], OutputStream out) throws IOException {
      int target = Integer.valueOf(args[0]);
      Collection<TaskTracker> enabled = jobTracker.getAliveTaskTrackers();
      for (TaskTracker node : enabled) {
         out.write("TT: ".getBytes());
         out.write(node.getHostname().getBytes());
         out.write("\n".getBytes());
      }

      out.write("@@@...".getBytes());

      if (enabled.size() == target) {
         return SUCCESS;
      }

      /* delay to emulate the fact that the script on the job tracker takes some time, no delay causes problems in getActiveTTs */
      try {
         Thread.sleep(10);
      } catch (InterruptedException e) {/*squash*/}

      return enabled.size() < target ? ERROR_FEWER_TTS : ERROR_EXCESS_TTS;
   }

   /**
    * The list file is a newline separated list of hostnames
    * @param args listfile, excludesFile, hadoopHome
    * @param out
    * @return
    */
   protected int shUpdateTTs(String args[], OutputStream out, boolean enable) {
      String list = files.get(args[0]);
      if (list == null) {
         return ERROR_DRLIST_FILE_NOT_FOUND;
      }

      Integer custom = null;
      int returnVal = SUCCESS;

      String hostnames[] = list.split("\n");
      for (String hostname : hostnames) {
         String result = enable ? jobTracker.enable(hostname.trim()) : jobTracker.disable(hostname.trim());
         if (result == null) {
            /* success */
            continue;
         } else if (result.equals(JobTracker.UNKNOWN_HOSTNAME_FOR_TASK_TRACKER)) {
            if (enable) {
               /* we expect not to know about new task trackers */
               continue;
            }

            return UNKNOWN_ERROR;
         } else if (result.equals(JobTracker.TASK_TRACKER_ALREADY_IN_TARGET_STATE)) {
            returnVal = enable ? WARN_TT_ACTIVE : WARN_TT_EXCLUDESFILE;
         } else if (result.equals(JobTracker.TASK_TRACKER_IN_UNDETERMINED_STATE)) {
            return UNKNOWN_ERROR;
         } else {
            /* try turning it into a return code */
            returnVal = UNKNOWN_ERROR;
            try {
               custom = Integer.valueOf(result);
            } catch (NumberFormatException e) {}
         }
      }

      return custom != null ? custom : returnVal;
   }
}
