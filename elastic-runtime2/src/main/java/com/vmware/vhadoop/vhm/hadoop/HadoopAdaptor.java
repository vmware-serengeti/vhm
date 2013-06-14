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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import com.vmware.vhadoop.api.vhm.HadoopActions;
import com.vmware.vhadoop.util.CompoundStatus;
import com.vmware.vhadoop.util.CompoundStatus.TaskStatus;
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

   private Map<String, HadoopConnection> _connections;
   private HadoopErrorCodes _errorCodes;
   private HadoopCredentials _credentials;
   private JTConfigInfo _jtConfig;
   private HadoopConnectionProperties _connectionProperties;        /* TODO: Provide setter? If not, make local */
   private Map<String, Map<ParamTypes, String>> _errorParamValues;               /* TODO: Will need one per connection/cluster */

   /* TODO: I think it's ok that these are all constants for now. Easy to externalize in future though */

   public static final int DEFAULT_SSH_PORT = 22;
   public static final String DEFAULT_SCP_READ_PERMS = "644";
   public static final String DEFAULT_SCP_EXECUTE_PERMS = "755";

   public static final String DECOM_LIST_FILE_NAME = "dlist.txt";
   public static final String DECOM_SCRIPT_FILE_NAME = "decommissionTTs.sh";
   public static final String RECOM_LIST_FILE_NAME = "rlist.txt";
   public static final String RECOM_SCRIPT_FILE_NAME = "recommissionTTs.sh";
   public static final String CHECK_SCRIPT_FILE_NAME = "checkTargetTTsSuccess.sh";

   /* TODO: Option to change the default values? */
   public static final String DEFAULT_SCRIPT_SRC_PATH = "src/main/resources/";
   public static final String DEFAULT_SCRIPT_DEST_PATH = "/tmp/";

   public static final String STATUS_GET_ACTIVE_TTS = "getActiveTTs";
   public static final String STATUS_INTERPRET_ERROR_CODE = "interpretErrorCode";

   public static final int MAX_CHECK_RETRY_ITERATIONS = 10;

   public HadoopAdaptor(HadoopCredentials credentials, JTConfigInfo jtConfig) {
      _connectionProperties = getDefaultConnectionProperties();
      _credentials = credentials;
      _jtConfig = jtConfig;
      _errorCodes = new HadoopErrorCodes();
      _errorParamValues = new HashMap<String, Map<ParamTypes, String>>();
      _connections = new HashMap<String, HadoopConnection>();
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
   public CompoundStatus decommissionTTs(Map<String, String> tts, HadoopClusterInfo cluster) {
      return decomRecomTTs("Decommission", tts, cluster, DECOM_SCRIPT_FILE_NAME, DECOM_LIST_FILE_NAME);
   }

   @Override
   public CompoundStatus recommissionTTs(Map<String, String> tts, HadoopClusterInfo cluster) {
      return decomRecomTTs("Recommission", tts, cluster, RECOM_SCRIPT_FILE_NAME, RECOM_LIST_FILE_NAME);
   }

   @Override
   public String[] getActiveTTs(HadoopClusterInfo cluster, int totalTargetEnabled, CompoundStatus status) {
      HadoopConnection connection = getConnectionForCluster(cluster);
      OutputStream out = new ByteArrayOutputStream();
      int rc = executeScriptWithCopyRetryOnFailure(connection, CHECK_SCRIPT_FILE_NAME, new String[]{""+totalTargetEnabled, connection.getExcludeFilePath(), connection.getHadoopHome()}, out);

      /* Convert to String array and "nullify" last element (which happens to be "@@@..." or empty line) */
      String[] unformattedList = out.toString().split("\n");
      String[] formattedList = new String[unformattedList.length - 1];
      for (int i = 0; i < formattedList.length; i++) {
         formattedList[i] = unformattedList[i].trim();
      }

      status.addStatus(_errorCodes.interpretErrorCode(_log, rc, getErrorParamValues(cluster)));
      return formattedList;
   }

//   @Override
//   public CompoundStatus checkTargetTTsSuccess(String opType, String[] affectedTTs, int totalTargetEnabled, HadoopClusterInfo cluster) {
//      CompoundStatus status = new CompoundStatus("checkTargetTTsSuccess");
//
//      String scriptFileName = CHECK_SCRIPT_FILE_NAME;
//      String scriptRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + scriptFileName;
//      String listRemoteFilePath = null;
//      String opDesc = "checkTargetTTsSuccess";
//
//      _log.log(Level.INFO, "Affected TTs:"+Arrays.asList(affectedTTs));
//
//      HadoopConnection connection = getConnectionForCluster(cluster);
//      setErrorParamsForCommand(cluster, opDesc, scriptRemoteFilePath, listRemoteFilePath);
//
//      int rc = -1;
//      int iterations = 0;
//      do {
//         if (iterations > 0) {
//          _log.log(Level.INFO, "Target TTs not yet achieved...checking again - " + iterations);
//         }
//
//         OutputStream out = new ByteArrayOutputStream();
//         rc = executeScriptWithCopyRetryOnFailure(connection, scriptFileName, new String[]{""+totalTargetEnabled, connection.getExcludeFilePath(), connection.getHadoopHome()}, out);
//
//         /* Convert to String array and "nullify" last element (which happens to be "@@@..." or empty line) */
//         String[] allActiveTTs = out.toString().split("\n");
//         allActiveTTs[allActiveTTs.length - 1] = null;
//
//         if (checkOpSuccess(opType, affectedTTs, allActiveTTs)) {
//            _log.log(Level.INFO, "All selected TTs correctly %sed", opType.toLowerCase());
//            rc = SUCCESS;
//            break;
//         }
//
//      } while ((rc == ERROR_FEWER_TTS || rc == ERROR_EXCESS_TTS) && (++iterations <= MAX_CHECK_RETRY_ITERATIONS));
//
//      status.addStatus(_errorCodes.interpretErrorCode(_log, rc, getErrorParamValues(cluster)));
//      return status;
//   }

   @Override
   public CompoundStatus checkTargetTTsSuccess(String opType, Map<String, String> affectedTTs, int totalTargetEnabled, HadoopClusterInfo cluster) {
      CompoundStatus status = new CompoundStatus("checkTargetTTsSuccess");

      String[] dnsNameArray = affectedTTs.values().toArray(new String[0]);
      String scriptRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + CHECK_SCRIPT_FILE_NAME;
      String listRemoteFilePath = null;
      String opDesc = "checkTargetTTsSuccess";

	   _log.log(Level.INFO, "Affected TTs:"+Arrays.asList(dnsNameArray));

      setErrorParamsForCommand(cluster, opDesc, scriptRemoteFilePath, listRemoteFilePath);

      int iterations = 0;
      CompoundStatus getActiveStatus = null;
      int rc = 0;
      boolean keepTrying = false;
      do {
    	   if (iterations > 0) {
    	    _log.log(Level.INFO, "Target TTs not yet achieved...checking again - " + iterations);
         }

         getActiveStatus = new CompoundStatus(STATUS_GET_ACTIVE_TTS);
    	   String[] allActiveTTs = getActiveTTs(cluster, totalTargetEnabled, getActiveStatus);

         if (getActiveStatus.getFirstFailure() == null) {
            _log.log(Level.INFO, "All selected TTs correctly %sed", opType.toLowerCase());
        	   break;
         }

         /* If there was an error reported by getActiveTTs... */
         TaskStatus taskStatus = getActiveStatus.getFirstFailure(STATUS_INTERPRET_ERROR_CODE);
         if (taskStatus != null) {
            rc = taskStatus.getErrorCode();

            /* If the error is simply that the TT count is not yet complete, keep trying
             *  and note the active TTs in the CompoundStats if we end up timing out */
            if ((rc == ERROR_FEWER_TTS || rc == ERROR_EXCESS_TTS)) {
               keepTrying = true;
               String incompleteVmList = null;
               if (rc == ERROR_FEWER_TTS) {
                  /* We're likely recommissioning so we want to report on the VMs that have not yet become active
                   * This means finding the VMs in affectedTTs that are not in allActiveTTs and mapping that back to the vmIds */
                  incompleteVmList = listIncompleteVmIdsForRecommission(affectedTTs, allActiveTTs).toString();
               } else {
                  /* We're likely decomissioning so we want to report on the VMs that are still active
                   * This means finding the union of affectedTTs and allActiveTTs and mapping that back to the vmIds */
                  incompleteVmList = listIncompleteVmIdsForDecommission(affectedTTs, allActiveTTs).toString();
               }
               getActiveStatus.registerTaskIncomplete(false, incompleteVmList, rc);
               _log.info("Building incomplete list of VMs: "+incompleteVmList);
            }
         }
      } while (keepTrying && (++iterations <= MAX_CHECK_RETRY_ITERATIONS));

      status.addStatus(getActiveStatus);
      return status;
   }

   Map<String, String> buildReverseLookup(Map<String, String> vmIdToDnsName) {
      Map<String, String> result = new HashMap<String, String>();
      for (String vmId : vmIdToDnsName.keySet()) {
         result.put(vmIdToDnsName.get(vmId), vmId);
      }
      return result;
   }

   private List<String> listIncompleteVmIdsForRecommission(Map<String, String> vmsShouldBeActive, String[] allActiveTTs) {
      List<String> result = new ArrayList<String>();
      Map<String, String> dnsNameToVmId = buildReverseLookup(vmsShouldBeActive);
      for (String activeTT : allActiveTTs) {
         dnsNameToVmId.remove(activeTT);
      }
      result.addAll(dnsNameToVmId.values());
      return result;
   }

   private List<String> listIncompleteVmIdsForDecommission(Map<String, String> vmsShouldNotBeActive, String[] allActiveTTs) {
      List<String> result = new ArrayList<String>();
      Map<String, String> dnsNameToVmId = buildReverseLookup(vmsShouldNotBeActive);
      for (String activeTT : allActiveTTs) {
         String vmId = dnsNameToVmId.get(activeTT);
         if (vmId != null) {
            result.add(vmId);
         }
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
