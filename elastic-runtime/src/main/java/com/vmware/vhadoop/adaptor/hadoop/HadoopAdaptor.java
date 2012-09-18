package com.vmware.vhadoop.adaptor.hadoop;

import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ERROR_COMMAND_NOT_FOUND;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ERROR_FEWER_TTS;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ERROR_EXCESS_TTS;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

/**
 * Class which represents the real implementation of HadoopActions
 * The class should be able to deal with multiple clusters and should have a HadoopConnection instance for each one.
 * The specifics of SSH and SCP are all handled in the HadoopConnection
 * 
 * @author bcorrie
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
   
   public static final int MAX_CHECK_RETRY_ITERATIONS = 10;
      
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
      Set<String> temp = new TreeSet<String>();
      for (String tt : tts) {
         if (!temp.add(tt)) {
            break;
         }
      }
      if (temp.size() < tts.length) {
         _log.log(Level.SEVERE, "Error: TT list contains duplicates!");
         return false;
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

   private int executeScriptWithCopyRetryOnFailure(HadoopConnection connection, String scriptFileName, String[] scriptArgs) {
      int rc = -1;
      for (int i=0; i<2; i++) {
         rc = connection.executeScript(scriptFileName, DEFAULT_SCRIPT_DEST_PATH, scriptArgs);
         if (rc == ERROR_COMMAND_NOT_FOUND) {
        	 //Changed this to accommodate using jar file...
        	String fullLocalPath = HadoopAdaptor.class.getClassLoader().getResource(scriptFileName).getPath();            
            //byte[] scriptData = loadLocalScript(DEFAULT_SCRIPT_SRC_PATH + scriptFileName);
        	byte[] scriptData = loadLocalScript(fullLocalPath);
            if ((scriptData != null) && 
                  (connection.copyDataToJobTracker(scriptData, DEFAULT_SCRIPT_DEST_PATH, scriptFileName, true) == 0)) {
               continue;
            }
         }
         break;
      }
      return rc;
   }

   private boolean decomRecomTTs(String opDesc, String[] tts, HadoopCluster cluster, 
         String scriptFileName, String listFileName) {
      if (!isValidTTList(tts)) {
         _log.log(Level.SEVERE, opDesc+" failed due to bad TT list");
         return false;
      }
      
      String scriptRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + scriptFileName;
      String listRemoteFilePath = DEFAULT_SCRIPT_DEST_PATH + listFileName;
      
      HadoopConnection connection = getConnectionForCluster(cluster);
      setErrorParamsForCommand(opDesc.toLowerCase(), scriptRemoteFilePath, listRemoteFilePath);
      
      String operationList = createVMList(tts);
      int rc = connection.copyDataToJobTracker(operationList.getBytes(), DEFAULT_SCRIPT_DEST_PATH, listFileName, false);
      if (rc == 0) {
         rc = executeScriptWithCopyRetryOnFailure(connection, scriptFileName, 
               new String[]{listRemoteFilePath, connection.getExcludeFilePath(), connection.getHadoopHome()});
      }
      return _errorCodes.interpretErrorCode(_log, rc, _errorParamValues);
   }
   
   @Override
   public boolean decommissionTTs(String[] tts, HadoopCluster cluster) {
      return decomRecomTTs("Decommission", tts, cluster, DECOM_SCRIPT_FILE_NAME, DECOM_LIST_FILE_NAME);
   }

   @Override
   public boolean recommissionTTs(String[] tts, HadoopCluster cluster) {
      return decomRecomTTs("Recommission", tts, cluster, RECOM_SCRIPT_FILE_NAME, RECOM_LIST_FILE_NAME);
   }

   @Override
   public boolean checkTargetTTsSuccess(int totalTargetEnabled, HadoopCluster cluster) {
      HadoopConnection connection = getConnectionForCluster(cluster);
      int rc = -1;
      int iterations = 0;
      do {
         rc = executeScriptWithCopyRetryOnFailure(connection, CHECK_SCRIPT_FILE_NAME, 
               new String[]{""+totalTargetEnabled, connection.getHadoopHome()});
      } while ((rc == ERROR_FEWER_TTS || rc == ERROR_EXCESS_TTS) && (++iterations <= MAX_CHECK_RETRY_ITERATIONS));
      return _errorCodes.interpretErrorCode(_log, rc, _errorParamValues);
   }
}
