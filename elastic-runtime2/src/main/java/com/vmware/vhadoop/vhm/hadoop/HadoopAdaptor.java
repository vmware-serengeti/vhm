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

import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_CATCHALL;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_COMMAND_NOT_FOUND;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_EXCESS_TTS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ERROR_FEWER_TTS;
import static com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.SUCCESS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.api.vhm.strategy.EDPolicy;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.CompoundStatus.TaskStatus;
import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.hadoop.HadoopConnection.HadoopConnectionProperties;
import com.vmware.vhadoop.vhm.hadoop.HadoopConnection.HadoopCredentials;
import com.vmware.vhadoop.vhm.hadoop.HadoopErrorCodes.ParamTypes;

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
   private final HadoopCredentials _credentials;
   private final JTConfigInfo _jtConfig;
   private final HadoopConnectionProperties _connectionProperties;        /* TODO: Provide setter? If not, make local */
   private final Map<String, Map<ParamTypes, String>> _errorParamValues;  /* TODO: Will need one per connection/cluster */
   private final ThreadLocalCompoundStatus _threadLocalStatus;

   /* TODO: I think it's ok that these are all constants for now. Easy to externalize in future though */

   private static final int DEFAULT_SSH_PORT = 22;
   private static final String DEFAULT_SCP_READ_PERMS = "644";
   private static final String DEFAULT_SCP_EXECUTE_PERMS = "755";

   private static final String DECOM_LIST_FILE_NAME = "dlist.txt";
   private static final String DECOM_SCRIPT_FILE_NAME = "decommissionTTs.sh";
   private static final String RECOM_LIST_FILE_NAME = "rlist.txt";
   private static final String RECOM_SCRIPT_FILE_NAME = "recommissionTTs.sh";
   private static final String CHECK_SCRIPT_FILE_NAME = "checkTargetTTsSuccess.sh";

   /* TODO: Option to change the default values? */
   private static final String DEFAULT_SCRIPT_SRC_PATH = "src/main/resources/";
   private static final String DEFAULT_SCRIPT_DEST_PATH = "/tmp/";

   static final String STATUS_INTERPRET_ERROR_CODE = "interpretErrorCode";

   private static final int MAX_CHECK_RETRY_ITERATIONS = 8;
   private static final long MIN_ACTIVE_TT_POLL_TIME_MILLIS = 1000;

   public HadoopAdaptor(HadoopCredentials credentials, JTConfigInfo jtConfig, ThreadLocalCompoundStatus tlcs) {
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
            return DEFAULT_SSH_PORT;
         }
         @Override
         public String getScpReadPerms() {
            return DEFAULT_SCP_READ_PERMS;
         }
         @Override
         public String getScpExecutePerms() {
            return DEFAULT_SCP_EXECUTE_PERMS;
         }
      };
   }

   private HadoopConnection getConnectionForCluster(HadoopClusterInfo cluster) {
      HadoopConnection result = _connections.get(cluster.getClusterId());
      if (result == null) {
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

   private boolean isValidTTList(String[] tts) {
      if (tts.length == 0) {
         _log.log(Level.SEVERE, "Error: Length of the TT list is 0!");
         return false;
      }

      _log.log(Level.INFO, "TTs length: " + tts.length);

	  Set<String> temp = new TreeSet<String>();
      for (String tt : tts) {
    	 if (tt == null) {
    		  _log.log(Level.SEVERE, "Error: Null TT name found while de/recommisioning");
    		  return false;
    	 }
    	 if (tt.length() == 0) {
   		      _log.log(Level.SEVERE, "Error: Empty TT name found while de/recommisioning");
   		      return false;
    	 }
		 if (!temp.add(tt)) {
			 _log.log(Level.SEVERE, "Error: TT list contains duplicates!");
			 return false;
		 }
      }

      return true;
   }

   private String createVMList(String[] tts) {
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
		   _log.log(Level.SEVERE, "File "+ fileName + " does not exist!");
		   return null;
	   }

	   byte[] result = null;
	   try {
		   result = IOUtils.toByteArray(is);
	   } catch (IOException e) {
		   _log.log(Level.SEVERE, "Unexpected error while converting file " + fileName + " to byte array", e);
	   }

      try {
         is.close();
      } catch (IOException e) {
         _log.fine("IOException while closing stream for loading script files");
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
         rc = connection.executeScript(scriptFileName, DEFAULT_SCRIPT_DEST_PATH, scriptArgs, out);
         if (i == 0 && (rc == ERROR_COMMAND_NOT_FOUND || rc == ERROR_CATCHALL)) {
            _log.log(Level.INFO, scriptFileName + " not found...");
            // Changed this to accommodate using jar file...
            // String fullLocalPath = HadoopAdaptor.class.getClassLoader().getResource(scriptFileName).getPath();
            // byte[] scriptData = loadLocalScript(DEFAULT_SCRIPT_SRC_PATH + scriptFileName);
            // byte[] scriptData = loadLocalScript(fullLocalPath);
            byte[] scriptData = loadLocalScript(scriptFileName);
            if ((scriptData != null) && (connection.copyDataToJobTracker(scriptData, DEFAULT_SCRIPT_DEST_PATH, scriptFileName, true) == 0)) {
               continue;
            }
         }
         break;
      }
      return rc;
   }

   private CompoundStatus decomRecomTTs(String opDesc, Map<String, String> tts, HadoopClusterInfo cluster, String scriptFileName, String listFileName) {
      CompoundStatus status = new CompoundStatus("decomRecomTTs");

      String[] dnsNameArray = tts.values().toArray(new String[0]);
      if (!isValidTTList(dnsNameArray)) {
         String errorMsg = opDesc+" failed due to bad TT list";
         _log.log(Level.SEVERE, opDesc+" failed due to bad TT list");
         status.registerTaskFailed(false, errorMsg);
         return status;
      }

      String scriptRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + scriptFileName;
      String listRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + listFileName;

      HadoopConnection connection = getConnectionForCluster(cluster);
      setErrorParamsForCommand(cluster, opDesc.toLowerCase(), scriptRemoteFilePath, listRemoteFilePath);

      OutputStream out = new ByteArrayOutputStream();
      String operationList = createVMList(dnsNameArray);
      int rc = connection.copyDataToJobTracker(operationList.getBytes(), DEFAULT_SCRIPT_DEST_PATH, listFileName, false);
      if (rc == 0) {
         rc = executeScriptWithCopyRetryOnFailure(connection, scriptFileName, new String[]{listRemoteFilePath, connection.getExcludeFilePath(), connection.getHadoopHome()}, out);
      }
      status.addStatus(_errorCodes.interpretErrorCode(_log, rc, getErrorParamValues(cluster)));
      return status;
   }

   @Override
   public void decommissionTTs(Map<String, String> tts, HadoopClusterInfo cluster) {
      getCompoundStatus().addStatus(decomRecomTTs("Decommission", tts, cluster, DECOM_SCRIPT_FILE_NAME, DECOM_LIST_FILE_NAME));
   }

   @Override
   public void recommissionTTs(Map<String, String> tts, HadoopClusterInfo cluster) {
      getCompoundStatus().addStatus(decomRecomTTs("Recommission", tts, cluster, RECOM_SCRIPT_FILE_NAME, RECOM_LIST_FILE_NAME));
   }

   @Override
   public Set<String> getActiveTTs(HadoopClusterInfo cluster, int totalTargetEnabled) {
      return getActiveTTs(cluster, totalTargetEnabled, getCompoundStatus());
   }


   protected Set<String> getActiveTTs(HadoopClusterInfo cluster, int totalTargetEnabled, CompoundStatus status) {
      HadoopConnection connection = getConnectionForCluster(cluster);
      OutputStream out = new ByteArrayOutputStream();
      int rc = executeScriptWithCopyRetryOnFailure(connection, CHECK_SCRIPT_FILE_NAME, new String[]{""+totalTargetEnabled, connection.getExcludeFilePath(), connection.getHadoopHome()}, out);

      String[] unformattedList = out.toString().split("\n");
      Set<String> formattedList = new HashSet<String>();
      for (int i = 0; i < unformattedList.length-1; i++) {
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
   public Set<String> checkTargetTTsSuccess(String opType, Map<String, String> affectedTTs, int totalTargetEnabled, HadoopClusterInfo cluster) {
      Set<String> patients = new HashSet<String>(affectedTTs.values());
      String scriptRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + CHECK_SCRIPT_FILE_NAME;
      String listRemoteFilePath = null;
      String opDesc = "checkTargetTTsSuccess";

	   _log.log(Level.INFO, "Affected TTs:"+patients);

      setErrorParamsForCommand(cluster, opDesc, scriptRemoteFilePath, listRemoteFilePath);

      int iterations = 0;
      CompoundStatus getActiveStatus = null;
      int rc = 0;
      Set<String> allActiveTTs = null;
      boolean retryTest;
      do {
    	   if (iterations > 0) {
       	   _log.log(Level.INFO, "Target TTs not yet achieved...checking again - " + iterations);
       	   _log.log(Level.INFO, "Affected TTs:"+patients);
         }

         getActiveStatus = new CompoundStatus(EDPolicy.ACTIVE_TTS_STATUS_KEY);
         
         /* Time the invocation to ensure that we don't zoom round the retry loop if it completes too quickly */
         long activeTTsStartTime = System.currentTimeMillis();
    	   allActiveTTs = getActiveTTs(cluster, totalTargetEnabled, getActiveStatus);
    	   long activeTTsElapsedTime = System.currentTimeMillis() - activeTTsStartTime;

    	   //Declare success as long as the we manage to de/recommission only the TTs we set out to handle (rather than checking correctness for all TTs)
    	   if ((opType.equals("Recommission") && allActiveTTs.containsAll(patients)) || (opType.equals("Decommission") && patients.retainAll(allActiveTTs) && patients.isEmpty())) {
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
             * */
            rc = SUCCESS;
         }
         retryTest = (rc == ERROR_FEWER_TTS || rc == ERROR_EXCESS_TTS) && (++iterations <= MAX_CHECK_RETRY_ITERATIONS);

         if (retryTest) {
            if (activeTTsElapsedTime < MIN_ACTIVE_TT_POLL_TIME_MILLIS) {
               try {
                  Thread.sleep(MIN_ACTIVE_TT_POLL_TIME_MILLIS - activeTTsElapsedTime);
               } catch (InterruptedException e) {}
            }
         }

      } while (retryTest);

      getCompoundStatus().addStatus(_errorCodes.interpretErrorCode(_log, rc, getErrorParamValues(cluster)));
      if (rc != SUCCESS) {
         getActiveStatus.registerTaskFailed(false, "Check Test Failed");
         getCompoundStatus().addStatus(getActiveStatus);
      }

      return convertDnsNamesToVmIds(affectedTTs, allActiveTTs);
   }

   private Set<String> convertDnsNamesToVmIds(Map<String, String> vmIdToDnsName, Set<String> allActiveTTs) {
      Set<String> result = new HashSet<String>();
      Map<String, String> dnsNameToVmId = buildReverseLookup(vmIdToDnsName);
      for (String activeTT : allActiveTTs) {
         String vmId = dnsNameToVmId.get(activeTT);
         if (vmId != null) {
            result.add(vmId);
         }
      }
      return result;
   }

   private Map<String, String> buildReverseLookup(Map<String, String> vmIdToDnsName) {
      Map<String, String> result = new HashMap<String, String>();
      for (String vmId : vmIdToDnsName.keySet()) {
         result.put(vmIdToDnsName.get(vmId), vmId);
      }
      return result;
   }

   /**
    * Interception point for fault injection, etc.
    * @return
    */
   protected HadoopConnection getHadoopConnection(HadoopClusterInfo cluster, HadoopConnectionProperties properties) {
      return new HadoopConnection(cluster, properties, new NonThreadSafeSshUtils());
   }
}
