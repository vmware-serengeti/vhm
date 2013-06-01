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

package com.vmware.vhadoop.adaptor.hadoop;

import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ERROR_COMMAND_NOT_FOUND;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ERROR_CATCHALL;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ERROR_FEWER_TTS;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ERROR_EXCESS_TTS;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.SUCCESS;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;


import com.vmware.vhadoop.adaptor.hadoop.HadoopConnection.HadoopConnectionProperties;
import com.vmware.vhadoop.adaptor.hadoop.HadoopConnection.HadoopCredentials;
import com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes;
import com.vmware.vhadoop.external.HadoopActions;
import com.vmware.vhadoop.external.HadoopCluster;
import com.vmware.vhadoop.util.CompoundStatus;

import org.apache.commons.io.IOUtils;

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
   private JTConfig _jtConfig;
   private HadoopConnectionProperties _connectionProperties;        /* TODO: Provide setter? If not, make local */
   private Map<ParamTypes, String> _errorParamValues;               /* TODO: Will need one per connection/cluster */
 
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
   
   public static final int MAX_CHECK_RETRY_ITERATIONS = 15;
      
   public HadoopAdaptor(HadoopCredentials credentials, JTConfig jtConfig) {
      _connectionProperties = getDefaultConnectionProperties();
      _credentials = credentials;
      _jtConfig = jtConfig;
      _errorCodes = new HadoopErrorCodes();
      _errorParamValues = new HashMap<ParamTypes, String>();
      _connections = new HashMap<String, HadoopConnection>();
   }
   
   private void setErrorParamValue(ParamTypes paramType, String paramValue) {
      _errorParamValues.put(paramType, paramValue);
   }

   private HadoopConnectionProperties getDefaultConnectionProperties() {
      return new HadoopConnectionProperties() {
         public int getSshPort() {
            return DEFAULT_SSH_PORT;
         }
         public String getScpReadPerms() {
            return DEFAULT_SCP_READ_PERMS;
         }
         public String getScpExecutePerms() {
            return DEFAULT_SCP_EXECUTE_PERMS;
         }
      };
   }
   
   private HadoopConnection getConnectionForCluster(HadoopCluster cluster) {
      HadoopConnection result = _connections.get(cluster.getClusterName());
      if (result == null) {
         /* TODO: SshUtils could be a single shared thread-safe object or non threadsafe object per connection */
         result = new HadoopConnection(cluster, _connectionProperties, new NonThreadSafeSshUtils());
         result.setHadoopCredentials(_credentials);
         result.setHadoopExcludeTTPath(_jtConfig.getExcludeTTPath());
         result.setHadoopHomePath(_jtConfig.getHadoopHomePath());
         _connections.put(cluster.getClusterName(), result);
      }
      setErrorParamValue(ParamTypes.HADOOP_HOME, result.getHadoopHome());
      setErrorParamValue(ParamTypes.JOBTRACKER, result.getJobTrackerName());
      setErrorParamValue(ParamTypes.EXCLUDE_FILE, result.getExcludeFilePath());
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

   private void setErrorParamsForCommand(String command, String drScript, String drList) {
      setErrorParamValue(ParamTypes.COMMAND, command);
      setErrorParamValue(ParamTypes.DRSCRIPT, drScript);
      setErrorParamValue(ParamTypes.DRLIST, drList);
   }

   private byte[] loadLocalScript(String fileName) {
	   InputStream is = HadoopAdaptor.class.getClassLoader().getResourceAsStream(fileName);
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
      for (int i=0; i<2; i++) {
         rc = connection.executeScript(scriptFileName, DEFAULT_SCRIPT_DEST_PATH, scriptArgs, out);
         if (i == 0 && (rc == ERROR_COMMAND_NOT_FOUND || rc == ERROR_CATCHALL)) {
        	 _log.log(Level.INFO, scriptFileName + " not found...");
        	 //Changed this to accommodate using jar file...
        	//String fullLocalPath = HadoopAdaptor.class.getClassLoader().getResource(scriptFileName).getPath();
        	//byte[] scriptData = loadLocalScript(DEFAULT_SCRIPT_SRC_PATH + scriptFileName);
        	//byte[] scriptData = loadLocalScript(fullLocalPath);
        	byte[] scriptData = loadLocalScript(scriptFileName);
            if ((scriptData != null) && 
                  (connection.copyDataToJobTracker(scriptData, DEFAULT_SCRIPT_DEST_PATH, scriptFileName, true) == 0)) {
               continue;
            }
         }
         break;
      }
      return rc;
   }

   private CompoundStatus decomRecomTTs(String opDesc, String[] tts, HadoopCluster cluster, 
         String scriptFileName, String listFileName) {
      CompoundStatus status = new CompoundStatus("decomRecomTTs");
      
      if (!isValidTTList(tts)) {
         String errorMsg = opDesc+" failed due to bad TT list";
         _log.log(Level.SEVERE, opDesc+" failed due to bad TT list");
         status.registerTaskFailed(false, errorMsg);
         return status;
      }
      
      String scriptRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + scriptFileName;
      String listRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + listFileName;
      
      HadoopConnection connection = getConnectionForCluster(cluster);
      setErrorParamsForCommand(opDesc.toLowerCase(), scriptRemoteFilePath, listRemoteFilePath);
      
      OutputStream out = new ByteArrayOutputStream();
      String operationList = createVMList(tts);
      int rc = connection.copyDataToJobTracker(operationList.getBytes(), DEFAULT_SCRIPT_DEST_PATH, listFileName, false);
      if (rc == 0) {
         rc = executeScriptWithCopyRetryOnFailure(connection, scriptFileName, 
               new String[]{listRemoteFilePath, connection.getExcludeFilePath(), connection.getHadoopHome()},
               out);
      }
      status.addStatus(_errorCodes.interpretErrorCode(_log, rc, _errorParamValues));
      return status;
   }
   
   @Override
   public CompoundStatus decommissionTTs(String[] tts, HadoopCluster cluster) {
      return decomRecomTTs("Decommission", tts, cluster, DECOM_SCRIPT_FILE_NAME, DECOM_LIST_FILE_NAME);
   }

   @Override
   public CompoundStatus recommissionTTs(String[] tts, HadoopCluster cluster) {
      return decomRecomTTs("Recommission", tts, cluster, RECOM_SCRIPT_FILE_NAME, RECOM_LIST_FILE_NAME);
   }

   @Override
   public CompoundStatus checkTargetTTsSuccess(String opType, String[] affectedTTs, int totalTargetEnabled, HadoopCluster cluster) {
      CompoundStatus status = new CompoundStatus("checkTargetTTsSuccess");
      
	   String scriptFileName = CHECK_SCRIPT_FILE_NAME;
      String scriptRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + scriptFileName;
      String listRemoteFilePath = null;
      String opDesc = "checkTargetTTsSuccess";
    
	   _log.log(Level.INFO, "AffectedTTs:");
	   for (String tt : affectedTTs) {
		   _log.log(Level.INFO, tt);
	   }
      HadoopConnection connection = getConnectionForCluster(cluster);
      setErrorParamsForCommand(opDesc, scriptRemoteFilePath, listRemoteFilePath);
      
      int rc = -1;
      int iterations = 0;
      do {
    	   if (iterations > 0) {
    	    _log.log(Level.INFO, "Target TTs not yet achieved...checking again - " + iterations);
         }    	 

         OutputStream out = new ByteArrayOutputStream();
    	   rc = executeScriptWithCopyRetryOnFailure(connection, scriptFileName, 
               new String[]{""+totalTargetEnabled, connection.getHadoopHome()},
               out); 
         try {
            out.flush();
         } catch (IOException e) {
            String errorMsg = "Unexpected exception in SSH OutputStream ";
            _log.log(Level.WARNING, errorMsg, e);
            status.registerTaskFailed(false, errorMsg + e.getMessage());
         }

         //_log.log(Level.INFO, "Output from SSH script execution:\n"+out.toString());

         /* Convert to String array and "nullify" last element (which happens to be "@@@..." or empty line) */
         String[] allActiveTTs = out.toString().split("\n");
         allActiveTTs[allActiveTTs.length - 1] = null;
         
         if (checkOpSuccess(opType, affectedTTs, allActiveTTs)) {
            _log.log(Level.INFO, "All selected TTs correctly %sed", opType.toLowerCase());
            rc = SUCCESS;
        	   break;
         }
         //TODO: out.close()?
         
      } while ((rc == ERROR_FEWER_TTS || rc == ERROR_EXCESS_TTS) && (++iterations <= MAX_CHECK_RETRY_ITERATIONS));

      status.addStatus(_errorCodes.interpretErrorCode(_log, rc, _errorParamValues));
      return status;
   }

   private boolean checkOpSuccess(String opType, String[] affectedTTs,
		   String[] allActiveTTs) {
	
	  Set<String> setTTs = new TreeSet<String>();

	  _log.log(Level.INFO, "ActiveTTs:");
	  for (String tt : allActiveTTs) {
		  if (tt != null) {
			  setTTs.add(tt); //add if unique...
			  _log.log(Level.INFO, tt);
		  }
	   }
		   
	   for (String tt : affectedTTs) {		  
		   if (setTTs.contains(tt) && opType == "Decommission") {
			   return false;
		   } else if (!setTTs.contains(tt) && opType == "Recommission") {
			   return false;
		   }
	   }

	   return true;
   }
}
