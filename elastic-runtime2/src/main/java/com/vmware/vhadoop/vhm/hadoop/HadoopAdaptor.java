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

import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_CATCHALL;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_COMMAND_NOT_FOUND;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_EXCESS_TTS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_FEWER_TTS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.SUCCESS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.UNKNOWN_ERROR;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.CompoundStatus.TaskStatus;
import com.vmware.vhadoop.util.ExternalizedParameters;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.hadoop.HadoopConnection.HadoopConnectionProperties;
import com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ParamTypes;
import com.vmware.vhadoop.vhm.hadoop.SshUtilities.Credentials;

/**
 * Class which represents the real implementation of HadoopActions
 * The class should be able to deal with multiple clusters and should have a HadoopConnection instance for each one.
 * The specifics of SSH and SCP are all handled in the HadoopConnection
 *
 */
public class HadoopAdaptor implements HadoopActions {

   private static final Logger _log = Logger.getLogger(HadoopAdaptor.class.getName());

   private final Map<String, HadoopConnection> _connections;
   private final HadoopErrorCodes _errorCodes;
   private final Credentials _credentials;
   private final JTConfigInfo _jtConfig;
   private final HadoopConnectionProperties _connectionProperties;        /* TODO: Provide setter? If not, make local */
   private final Map<String, Map<ParamTypes, String>> _errorParamValues;  /* TODO: Will need one per connection/cluster */
   private final ThreadLocalCompoundStatus _threadLocalStatus;

   private final int JOB_TRACKER_DEFAULT_SSH_PORT = ExternalizedParameters.get().getInt("JOB_TRACKER_DEFAULT_SSH_PORT");
   private final String JOB_TRACKER_SCP_READ_PERMS = ExternalizedParameters.get().getString("JOB_TRACKER_SCP_READ_PERMS");
   private final String JOB_TRACKER_SCP_EXECUTE_PERMS = ExternalizedParameters.get().getString("JOB_TRACKER_SCP_EXECUTE_PERMS");
   private final int JOB_TRACKER_SSH_CONNECTION_CACHE_SIZE = ExternalizedParameters.get().getInt("JOB_TRACKER_SSH_CONNECTION_CACHE_SIZE");

   private final String JOB_TRACKER_DECOM_LIST_FILE_NAME = ExternalizedParameters.get().getString("JOB_TRACKER_DECOM_LIST_FILE_NAME");
   private final String JOB_TRACKER_DECOM_SCRIPT_FILE_NAME = ExternalizedParameters.get().getString("JOB_TRACKER_DECOM_SCRIPT_FILE_NAME");
   private final String JOB_TRACKER_RECOM_LIST_FILE_NAME = ExternalizedParameters.get().getString("JOB_TRACKER_RECOM_LIST_FILE_NAME");
   private final String JOB_TRACKER_RECOM_SCRIPT_FILE_NAME = ExternalizedParameters.get().getString("JOB_TRACKER_RECOM_SCRIPT_FILE_NAME");
   private final String JOB_TRACKER_CHECK_SCRIPT_FILE_NAME = ExternalizedParameters.get().getString("JOB_TRACKER_CHECK_SCRIPT_FILE_NAME");

   private final String DEFAULT_SCRIPT_SRC_PATH = ExternalizedParameters.get().getString("DEFAULT_SCRIPT_SRC_PATH");
   private final String JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH = ExternalizedParameters.get().getString("JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH");

   private final int ACTIVE_TASK_TRACKERS_CHECK_RETRY_ITERATIONS = ExternalizedParameters.get().getInt("ACTIVE_TASK_TRACKERS_CHECK_RETRY_ITERATIONS");;

   static final String STATUS_INTERPRET_ERROR_CODE = "interpretErrorCode";
   public static final String ACTIVE_TTS_STATUS_KEY = "getActiveStatus";

   public HadoopAdaptor(Credentials credentials, JTConfigInfo jtConfig, ThreadLocalCompoundStatus tlcs) {
      _connectionProperties = getDefaultConnectionProperties();
      _credentials = credentials;
      _jtConfig = jtConfig;
      _errorCodes = new HadoopErrorCodes();
      _errorParamValues = new HashMap<String, Map<ParamTypes, String>>();
      _connections = new HashMap<String, HadoopConnection>();
      _threadLocalStatus = tlcs;
   }

   private CompoundStatus getCompoundStatus() {
      if (_threadLocalStatus == null) {
         return new CompoundStatus("DUMMY_STATUS");
      }
      return _threadLocalStatus.get();
   }

   private void setErrorParamValue(HadoopClusterInfo cluster, ParamTypes paramType, String paramValue) {
      Map<ParamTypes, String> paramValues = _errorParamValues.get(cluster.getClusterId());
      if (paramValues == null) {
         paramValues = new HashMap<ParamTypes, String>();
         _errorParamValues.put(cluster.getClusterId(), paramValues);
      }
      paramValues.put(paramType, paramValue);
   }

   private Map<ParamTypes, String> getErrorParamValues(HadoopClusterInfo cluster) {
      return _errorParamValues.get(cluster.getClusterId());
   }

   private HadoopConnectionProperties getDefaultConnectionProperties() {
      return new HadoopConnectionProperties() {
         @Override
         public int getSshPort() {
            return JOB_TRACKER_DEFAULT_SSH_PORT;
         }
         @Override
         public String getScpReadPerms() {
            return JOB_TRACKER_SCP_READ_PERMS;
         }
         @Override
         public String getScpExecutePerms() {
            return JOB_TRACKER_SCP_EXECUTE_PERMS;
         }
      };
   }

   private HadoopConnection getConnectionForCluster(HadoopClusterInfo cluster) {
      if ((cluster == null) || (cluster.getJobTrackerDnsName() == null)) {
         return null;
      }
      HadoopConnection result = _connections.get(cluster.getClusterId());
      if (result == null || result.isStale(cluster)) {
         /* TODO: SshUtils could be a single shared thread-safe object or non threadsafe object per connection */
         result = getHadoopConnection(cluster, _connectionProperties);
         result.setHadoopCredentials(_credentials);
         result.setHadoopExcludeTTPath(_jtConfig.getExcludeTTPath());
         result.setHadoopHomePath(_jtConfig.getHadoopHomePath());
         _connections.put(cluster.getClusterId(), result);
      }
      setErrorParamValue(cluster, ParamTypes.HADOOP_HOME, result.getHadoopHome());
      setErrorParamValue(cluster, ParamTypes.JOBTRACKER, result.getJobTrackerAddr());
      setErrorParamValue(cluster, ParamTypes.EXCLUDE_FILE, result.getExcludeFilePath());
      return result;
   }

   private boolean isValidTTList(Set<String> ttDnsNames) {
      if ((ttDnsNames == null) || (ttDnsNames.isEmpty())) {
         _log.log(Level.SEVERE, "VHM: validating task tracker list failed while de/recommisioning - the list is empty");
         return false;
      }

      for (String tt : ttDnsNames) {
         if (tt == null) {
            _log.log(Level.SEVERE, "VHM: validating task tracker list failed while de/recommisioning - null task tracker name");
            return false;
         }
         if (tt.length() == 0) {
            _log.log(Level.SEVERE, "VHM: validating task tracker list failed while de/recommisioning - blank task tracker name");
            return false;
         }
      }

      return true;
   }

   private String createVMList(Set<String> tts) {
      StringBuilder sb = new StringBuilder();
      for (String tt : tts) {
         sb.append(tt).append('\n');
      }
      return sb.toString();
   }

   private void setErrorParamsForCommand(HadoopClusterInfo cluster, String command, String drScript, String drList) {
      setErrorParamValue(cluster, ParamTypes.COMMAND, command);
      setErrorParamValue(cluster, ParamTypes.DRSCRIPT, drScript);
      setErrorParamValue(cluster, ParamTypes.DRLIST, drList);
   }

   private byte[] loadLocalScript(String fileName) {
      ClassLoader cl = HadoopAdaptor.class.getClassLoader();
	   InputStream is = ((cl != null) && (fileName != null)) ? cl.getResourceAsStream(fileName) : null;
	   if (is == null) {
		   _log.log(Level.SEVERE, "VHM: class loader resource "+ fileName + " is unavailable");
		   return null;
	   }

	   byte[] result = null;
	   try {
		   result = IOUtils.toByteArray(is);
	   } catch (IOException e) {
		   _log.log(Level.SEVERE, "VHM: exception converting class loader resource "+ fileName + " to byte array", e);
	   }

      try {
         is.close();
      } catch (IOException e) {
         _log.fine("VHM: exception closing stream for class loader resource " + fileName);
      }

	   return result;
   }

   /*
   private byte[] loadLocalScript(String fullLocalPath) {
      File file = new File(fullLocalPath);
      if (!file.exists()) {
         _log.log(Level.SEVERE, "File "+fullLocalPath+" does not exist!");
         return null;
      }
      try {
         FileInputStream fis = new FileInputStream(file);
         BufferedInputStream bis = new BufferedInputStream(fis);
         byte[] result = new byte[(int)file.length()];
         bis.read(result);
         bis.close();
         fis.close();
         return result;
      } catch (IOException e) {
         _log.log(Level.SEVERE, "Unexpected error reading file "+fullLocalPath, e);
      }
      return null;
   }
*/

   private int executeScriptWithCopyRetryOnFailure(HadoopConnection connection, String scriptFileName, String[] scriptArgs, OutputStream out) {
      int rc = -1;
      for (int i = 0; i < 2; i++) {
         rc = connection.executeScript(scriptFileName, JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH, scriptArgs, out);
         if (i == 0 && (rc == ERROR_COMMAND_NOT_FOUND || rc == ERROR_CATCHALL)) {
            _log.log(Level.INFO, scriptFileName + " not found...");
            // Changed this to accommodate using jar file...
            // String fullLocalPath = HadoopAdaptor.class.getClassLoader().getResource(scriptFileName).getPath();
            // byte[] scriptData = loadLocalScript(DEFAULT_SCRIPT_SRC_PATH + scriptFileName);
            // byte[] scriptData = loadLocalScript(fullLocalPath);
            byte[] scriptData = loadLocalScript(scriptFileName);
            if ((scriptData != null) && (connection.copyDataToJobTracker(scriptData, JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH, scriptFileName, true) == 0)) {
               continue;
            }
         }
         break;
      }
      return rc;
   }

   private CompoundStatus decomRecomTTs(String opDesc, Set<String> ttDnsNames, HadoopClusterInfo cluster, String scriptFileName, String listFileName) {
      CompoundStatus status = new CompoundStatus("decomRecomTTs");

      if (!isValidTTList(ttDnsNames)) {
         String errorMsg = opDesc+" failed due to bad task tracker list";
         _log.log(Level.SEVERE, "<%C"+cluster.getClusterId()+"%C>: "+errorMsg);
         status.registerTaskFailed(false, errorMsg);
         return status;
      }

      String scriptRemoteFilePath = JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH + scriptFileName;
      String listRemoteFilePath = JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH + listFileName;

      HadoopConnection connection = getConnectionForCluster(cluster);
      if (connection != null) {
         setErrorParamsForCommand(cluster, opDesc.toLowerCase(), scriptRemoteFilePath, listRemoteFilePath);

         OutputStream out = new ByteArrayOutputStream();
         String operationList = createVMList(ttDnsNames);
         int rc = connection.copyDataToJobTracker(operationList.getBytes(), JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH, listFileName, false);
         if (rc == 0) {
            rc = executeScriptWithCopyRetryOnFailure(connection, scriptFileName, new String[]{listRemoteFilePath, connection.getExcludeFilePath(), connection.getHadoopHome()}, out);
         }
         status.addStatus(_errorCodes.interpretErrorCode(_log, rc, getErrorParamValues(cluster)));
      } else {
         status.registerTaskFailed(false, "could not create connection to job tracker for cluster");
      }
      return status;
   }

   @Override
   public void decommissionTTs(Set<String> ttDnsNames, HadoopClusterInfo cluster) {
      getCompoundStatus().addStatus(decomRecomTTs("Decommission", ttDnsNames, cluster, JOB_TRACKER_DECOM_SCRIPT_FILE_NAME, JOB_TRACKER_DECOM_LIST_FILE_NAME));
   }

   @Override
   public void recommissionTTs(Set<String> ttDnsNames, HadoopClusterInfo cluster) {
      getCompoundStatus().addStatus(decomRecomTTs("Recommission", ttDnsNames, cluster, JOB_TRACKER_RECOM_SCRIPT_FILE_NAME, JOB_TRACKER_RECOM_LIST_FILE_NAME));
   }

   @Override
   public Set<String> getActiveTTs(HadoopClusterInfo cluster, int totalTargetEnabled) {
      return getActiveTTs(cluster, totalTargetEnabled, getCompoundStatus());
   }


   protected Set<String> getActiveTTs(HadoopClusterInfo cluster, int totalTargetEnabled, CompoundStatus status) {
      HadoopConnection connection = getConnectionForCluster(cluster);
      if (connection == null) {
         return null;
      }
      OutputStream out = new ByteArrayOutputStream();
      int rc = executeScriptWithCopyRetryOnFailure(connection, JOB_TRACKER_CHECK_SCRIPT_FILE_NAME, new String[]{""+totalTargetEnabled, connection.getExcludeFilePath(), connection.getHadoopHome()}, out);

      _log.info("Error code from executing script " + rc);

      String[] unformattedList = out.toString().split("\n");
      Set<String> formattedList = new HashSet<String>(); //Note: set also avoids potential duplicate TTnames (e.g., when a TT is restarted without decommissioning)
      /* JG: Changing for-loop limit from unformattedList.length-1 to unformattedList.length since we now explicitly check for TTnames starting with "TT:" (No more @@@... issue) */
      for (int i = 0; i < unformattedList.length; i++) {
         //Expecting TTs to be annotated as "TT: ttName"
         if (unformattedList[i].startsWith("TT:")) {
            _log.fine("Adding TT: " + unformattedList[i].split("\\s+")[1]);
            formattedList.add(unformattedList[i].split("\\s+")[1]);
         }
         //formattedList.add(unformattedList[i].trim());
      }

      _log.info("Active TTs so far: " + Arrays.toString(formattedList.toArray()));
      _log.info("#Active TTs: " + formattedList.size() + "\t #Target TTs: " + totalTargetEnabled);
      status.addStatus(_errorCodes.interpretErrorCode(_log, rc, getErrorParamValues(cluster)));
      return formattedList;
   }

   @Override
   /* Returns the set of active dnsNames based on input Set */
   public Set<String> checkTargetTTsSuccess(String opType, Set<String> ttDnsNames, int totalTargetEnabled, HadoopClusterInfo cluster) {
      String scriptRemoteFilePath = JOB_TRACKER_DEFAULT_SCRIPT_DEST_PATH + JOB_TRACKER_CHECK_SCRIPT_FILE_NAME;
      String listRemoteFilePath = null;
      String opDesc = "checkTargetTTsSuccess";

      if (ttDnsNames == null) {
         _log.warning("No valid TT names provided");
         return null;
      }

      /* We don't expect null or empty values, but weed out anyway */
      ttDnsNames.remove(null);
      ttDnsNames.remove("");
      if (ttDnsNames.size() == 0) {
         _log.warning("No valid TT names provided");
         return null;
      }

	   _log.log(Level.INFO, "Affected TTs: "+ttDnsNames);

      setErrorParamsForCommand(cluster, opDesc, scriptRemoteFilePath, listRemoteFilePath);

      int iterations = 0;
      CompoundStatus getActiveStatus = null;
      int rc = UNKNOWN_ERROR;
      Set<String> allActiveTTs = null;
      do {
    	   if (iterations > 0) {
       	   _log.log(Level.INFO, "Target TTs not yet achieved...checking again - " + iterations);
       	   _log.log(Level.INFO, "Affected TTs: "+ttDnsNames);
         }

         getActiveStatus = new CompoundStatus(ACTIVE_TTS_STATUS_KEY);

    	   allActiveTTs = getActiveTTs(cluster, totalTargetEnabled, getActiveStatus);

    	   //Declare success as long as the we manage to de/recommission only the TTs we set out to handle (rather than checking correctness for all TTs)
    	   if ((allActiveTTs != null) &&
    	         ((opType.equals("Recommission") && allActiveTTs.containsAll(ttDnsNames)) ||
    	               (opType.equals("Decommission") && ttDnsNames.retainAll(allActiveTTs) && ttDnsNames.isEmpty()))) {
            _log.log(Level.INFO, "All selected TTs correctly %sed", opType.toLowerCase());
            rc = SUCCESS;
            break;
         }

         /* If there was an error reported by getActiveTTs... */
         TaskStatus taskStatus = getActiveStatus.getFirstFailure(STATUS_INTERPRET_ERROR_CODE);
         if (taskStatus != null) {
            rc = taskStatus.getErrorCode();
         } else {
            /*
             * JG: Sometimes we don't know the hostnames (e.g., localhost); in these cases as long as the check script returns success based
             * on target #TTs we are good.
             * TODO: Change check script to return success if #newly added + #current_enabled is met rather than target #TTs is met. This is
             * to address scenarios where there is a mismatch (#Active TTs != #poweredOn VMs) to begin with...
             * CHANGED: We have changed the time at which this function is invoked -- it gets invoked only when dns/hostnames are available.
             * So we no longer have this issue of not knowing hostnames and still meeting target #TTs. Our only successful exit is when the
             * TTs that have been explicitly asked to be checked, have been correctly de/recommissioned.
             *
             * rc = SUCCESS; //Note: removing this
             *
             * We also notice that in this case, where #Active TTs matches target, but all the requested TTs haven't been de/recommissioned yet,
             * the check script returns immediately (because it only looks for a match of these values, which is true here). So we recompute
             * target TTs based on latest information to essentially put back the delay...
             */

            Set<String> deltaTTs = new HashSet<String>(ttDnsNames);
            if (opType.equals("Recommission")) {
               deltaTTs.removeAll(allActiveTTs); //get TTs that haven't been recommissioned yet...
               totalTargetEnabled = allActiveTTs.size() + deltaTTs.size();
            } else { //optype = Decommission
               deltaTTs.retainAll(allActiveTTs); //get TTs that haven't been decommissioned yet...
               totalTargetEnabled = allActiveTTs.size() - deltaTTs.size();
            }

            _log.log(Level.INFO, "Even though #ActiveTTs = #TargetTTs, not all requested TTs have been " + opType.toLowerCase() + "ed yet - Trying again with updated target: " + totalTargetEnabled);
         }

         /* Break out if there is an error other than the ones we expect to be resolved in a subsequent invocation of the check script */
         if (rc != ERROR_FEWER_TTS && rc != ERROR_EXCESS_TTS && rc != UNKNOWN_ERROR) {
            break;
         }
      } while (iterations++ < ACTIVE_TASK_TRACKERS_CHECK_RETRY_ITERATIONS);

      getCompoundStatus().addStatus(_errorCodes.interpretErrorCode(_log, rc, getErrorParamValues(cluster)));
      if (rc != SUCCESS) {
         getActiveStatus.registerTaskFailed(false, "Check Test Failed");
         getCompoundStatus().addStatus(getActiveStatus);
      }

      return allActiveTTs;
   }

   /**
    * Interception point for fault injection, etc.
    * @return
    */
   protected HadoopConnection getHadoopConnection(HadoopClusterInfo cluster, HadoopConnectionProperties properties) {
      return new HadoopConnection(cluster, properties, new SshConnectionCache(JOB_TRACKER_SSH_CONNECTION_CACHE_SIZE));
   }

   @Override
   public boolean validateTtHostNames(Set<String> dnsNames) {
      return true;
   }
}
