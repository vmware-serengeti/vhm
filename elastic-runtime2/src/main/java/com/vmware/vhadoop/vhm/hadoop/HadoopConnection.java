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

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.jcraft.jsch.ChannelExec;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
/**
 * Encapsulates the details of SCPing data to a remote VM and executing scripts on it
 * Uses an SshUtils which contains the ugly utility code for managing SSH connections
 *
 */
public class HadoopConnection {

   private static final Logger _log = Logger.getLogger(HadoopConnection.class.getName());

   public interface HadoopCredentials {
      public String getSshUsername();
      public String getSshPassword();
      public String getSshPrvkeyFile();
   }

   public interface HadoopConnectionProperties {
      public int getSshPort();
      public String getScpReadPerms();
      public String getScpExecutePerms();
   }

   public interface SshUtils {
      ChannelExec createChannel(Logger logger, HadoopCredentials credentials, String host, int port);
      int exec(Logger logger, ChannelExec channel, OutputStream out, String command);
      int scpBytes(Logger logger, ChannelExec channel, byte[] data, String remotePath, String remoteFileName, String perms);
      boolean testChannel(Logger logger, ChannelExec channel);
      void cleanup(Logger logger, OutputStream out, ChannelExec channel);
   }

   private final HadoopConnectionProperties _connectionProperties;
   private final HadoopClusterInfo _hadoopCluster;
   private final SshUtils _sshUtils;

   /* These may all be optional, so all set using setters, not constructor */
   private HadoopCredentials _credentials;
   private String _hadoopHomePath;
   private String _hadoopExcludeTTPath;

   /* SshUtils can theoretically be shared by multiple HadoopConnection instances */
   public HadoopConnection(HadoopClusterInfo cluster, HadoopConnectionProperties props, SshUtils sshUtils) {
      _hadoopCluster = cluster;
      _connectionProperties = props;
      _sshUtils = sshUtils;
   }

   public void setHadoopCredentials(HadoopCredentials credentials) {
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

      _log.log(Level.INFO, "Copying data to remote file "+remotePath+remoteFileName + " on jobtracker " + _hadoopCluster.getJobTrackerIpAddr());

      if (_hadoopCluster.getJobTrackerIpAddr() == null) {
         return HadoopErrorCodes.ERROR_JT_CONNECTION;
      }

      ChannelExec channel = _sshUtils.createChannel(_log, _credentials, _hadoopCluster.getJobTrackerIpAddr(), _connectionProperties.getSshPort());
      if (channel == null) {
         return UNKNOWN_ERROR;          /* TODO: Improve */
      }

      String perms;
      if (isExecutable) {
         perms = _connectionProperties.getScpExecutePerms();
      } else {
         perms = _connectionProperties.getScpReadPerms();
      }

      try {
         exitStatus = _sshUtils.scpBytes(_log, channel, inputData, remotePath, remoteFileName, perms);
      } finally {
         _sshUtils.cleanup(_log, null, channel);
      }

      return exitStatus;
   }

   public int executeScript(String scriptFileName, String destinationPath, String[] scriptArgs, OutputStream out) {
      int exitStatus = UNKNOWN_ERROR;

      _log.log(Level.INFO, "Executing remote script: " + destinationPath + scriptFileName + " on jobtracker " + _hadoopCluster.getJobTrackerIpAddr());

      if (_hadoopCluster.getJobTrackerIpAddr() == null) {
         return HadoopErrorCodes.ERROR_JT_CONNECTION;
      }

      ChannelExec channel = null;
      PrintStream ps = null;
      try {
         channel = _sshUtils.createChannel(_log, _credentials, _hadoopCluster.getJobTrackerIpAddr(), _connectionProperties.getSshPort());
         if (channel == null) {
            return UNKNOWN_ERROR;          /* TODO: Improve */
         }
   
         StringBuilder command = new StringBuilder(destinationPath + scriptFileName).append(" ");
         for (String scriptArg : scriptArgs) {
            command.append(scriptArg).append(" ");
         }
   
         ps = new PrintStream(out);
         exitStatus = _sshUtils.exec(_log, channel, out, command.toString().trim());
      } finally {
         _sshUtils.cleanup(_log, null, channel);
         if (ps != null) {
            ps.flush();       /* Results in out.flush() */
         }
      }

      _log.log(Level.FINEST, "Output from SSH script execution:\n{0}\n", out.toString());

      _sshUtils.cleanup(_log, null, channel);

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
