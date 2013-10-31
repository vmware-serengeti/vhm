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

/***************************************************************************
 * Copyright (c) 2012 VMware, Inc. All Rights Reserved.
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

import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.UNKNOWN_ERROR;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.vhm.hadoop.SshUtilities.Credentials;
/**
 * Encapsulates the details of SCPing data to a remote VM and executing scripts on it
 * Uses an SshUtils which contains the ugly utility code for managing SSH connections
 *
 */
public class HadoopConnection {

   private static final Logger _log = Logger.getLogger(HadoopConnection.class.getName());

   public interface HadoopConnectionProperties {
      public int getSshPort();
      public String getScpReadPerms();
      public String getScpExecutePerms();
   }

   private final HadoopConnectionProperties _connectionProperties;
   private final HadoopClusterInfo _hadoopCluster;
   private final SshUtilities _sshUtils;

   /* These may all be optional, so all set using setters, not constructor */
   private Credentials _credentials;
   private String _hadoopHomePath;
   private String _hadoopExcludeTTPath;

   /* SshUtils can theoretically be shared by multiple HadoopConnection instances */
   public HadoopConnection(HadoopClusterInfo cluster, HadoopConnectionProperties props, SshUtilities sshUtils) {
      _hadoopCluster = cluster;
      _connectionProperties = props;
      _sshUtils = sshUtils;
   }

   public void setHadoopCredentials(Credentials credentials) {
      _credentials = credentials;
   }

   public void setHadoopHomePath(String hadoopHomePath) {
      _hadoopHomePath = hadoopHomePath;
   }

   public void setHadoopExcludeTTPath(String hadoopExcludeTTPath) {
      _hadoopExcludeTTPath = hadoopExcludeTTPath;
   }

   public int copyDataToJobTracker(byte[] inputData, String remotePath, String remoteFileName, boolean isExecutable) {
      int exitStatus = UNKNOWN_ERROR;

      _log.log(Level.INFO, "VHM: "+_hadoopCluster.getJobTrackerDnsName()+" - copying data to remote file "+remotePath+remoteFileName + " on jobtracker");

      if (_hadoopCluster.getJobTrackerDnsName() == null) {
         return HadoopErrorCodes.ERROR_JT_CONNECTION;
      }

      String perms = isExecutable ? _connectionProperties.getScpExecutePerms() : _connectionProperties.getScpReadPerms();

      try {
         exitStatus = _sshUtils.copy(_hadoopCluster.getJobTrackerDnsName(), _connectionProperties.getSshPort(), _credentials, inputData, remotePath, remoteFileName, perms);
      } catch (IOException e) {
         _log.info("VHM: "+_hadoopCluster.getJobTrackerDnsName()+" - failed to copy data to "+remotePath+remoteFileName);
      }

      return exitStatus;
   }

   public int executeScript(String scriptFileName, String destinationPath, String[] scriptArgs, OutputStream out) {
      int exitStatus = UNKNOWN_ERROR;

      _log.log(Level.INFO, "Executing remote script: " + destinationPath + scriptFileName + " on jobtracker " + _hadoopCluster.getJobTrackerIpAddr());

      if (_hadoopCluster.getJobTrackerDnsName() == null) {
         return HadoopErrorCodes.ERROR_JT_CONNECTION;
      }

      try {
         StringBuilder command = new StringBuilder("sudo "+destinationPath + scriptFileName).append(" ");
         for (String scriptArg : scriptArgs) {
            command.append(scriptArg).append(" ");
         }

         exitStatus = _sshUtils.execute(_hadoopCluster.getJobTrackerDnsName(), _connectionProperties.getSshPort(), _credentials, command.toString().trim(), out);
      } catch (IOException e) {
         _log.info("VHM: "+_hadoopCluster.getJobTrackerDnsName()+" - failed to execute command on target");
      }

      _log.log(Level.FINEST, "Output from SSH script execution:\n{0}\n", out.toString());

      return exitStatus;
   }

   public String getHadoopHome() {
      return _hadoopHomePath;
   }

   public boolean isStale(HadoopClusterInfo newClusterInfo) {
      return !_hadoopCluster.equals(newClusterInfo);
   }

   public String getJobTrackerAddr() {
      return _hadoopCluster.getJobTrackerIpAddr();
   }

   public String getExcludeFilePath() {
      return _hadoopExcludeTTPath;
   }

   @Override
   public String toString() {
      return _hadoopCluster.getClusterId();
   }
}
