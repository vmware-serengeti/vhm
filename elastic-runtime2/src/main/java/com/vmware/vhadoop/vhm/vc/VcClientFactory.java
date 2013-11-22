package com.vmware.vhadoop.vhm.vc;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.vmware.vhadoop.util.ExternalizedParameters;
import com.vmware.vim.vmomi.client.Client;

/**
 * This class is a thread-safe way of accessing and validating pre-created VCClients
 */
public class VcClientFactory {
   boolean _initialized = false;
   private static final Logger _log = Logger.getLogger(VcClientFactory.class.getName());

   private final long VC_CONTROL_CONNECTION_TIMEOUT_MILLIS = ExternalizedParameters.get().getLong("VC_CONTROL_CONNECTION_TIMEOUT_MILLIS");
   private final long VC_WAIT_FOR_UPDATES_CONNECTION_TIMEOUT_MILLIS = ExternalizedParameters.get().getLong("VC_WAIT_FOR_UPDATES_CONNECTION_TIMEOUT_MILLIS");  /* WaitForUpdates will block for at most this period */
   private final long VC_STATS_POLL_CONNECTION_TIMEOUT_MILLIS = ExternalizedParameters.get().getLong("VC_STATS_POLL_CONNECTION_TIMEOUT_MILLIS");   /* Stats collection timeout should be short */

   private final VcVlsi _vcVlsi;
   private final VcCredentials _vcCreds;
   private Client _controlClient;         // used for VC control operations and is the parent client for the others
   private Client _waitForUpdateClient;   // used for the main waitForPropertyChange loop
   private Client _statsPollClient;       // used for VC stats collection
   private String _waitForUpdatesVersion = "";

   private AtomicReference<Thread> _initiatingConnectionThread = new AtomicReference<Thread>();
   
   protected enum VcClientKey{CONTROL_CLIENT, WAIT_FOR_UPDATE_CLIENT, STATS_POLL_CLIENT};

   protected VcClientFactory(VcVlsi vcVlsi, VcCredentials vcCreds) {
      _vcVlsi = vcVlsi;
      _vcCreds = vcCreds;
   }
   
   /* Getting and setting of the clients is synchronized */
   private synchronized Client getClientForKey(VcClientKey clientKey) {
      if (clientKey.equals(VcClientKey.CONTROL_CLIENT)) {
         return _controlClient;
      } else
      if (clientKey.equals(VcClientKey.WAIT_FOR_UPDATE_CLIENT)) {
         return _waitForUpdateClient;
      } else
      if (clientKey.equals(VcClientKey.STATS_POLL_CLIENT)) {
         return _statsPollClient;
      }
      return null;
   }

   /* Getting and setting of the clients is synchronized */
   private synchronized void setClientForKey(VcClientKey clientKey, Client client) {
      if (clientKey.equals(VcClientKey.CONTROL_CLIENT)) {
         _controlClient = client;
      } else
      if (clientKey.equals(VcClientKey.WAIT_FOR_UPDATE_CLIENT)) {
         _waitForUpdateClient = client;
      } else
      if (clientKey.equals(VcClientKey.STATS_POLL_CLIENT)) {
         _statsPollClient = client;
      }
   }

   // returns true if it successfully connected to VC
   private boolean initClients(boolean useCert, Long customTimeout) {
      try {
         setClientForKey(VcClientKey.CONTROL_CLIENT, _vcVlsi.connect(_vcCreds, useCert, null, 
               (customTimeout != null ? customTimeout : VC_CONTROL_CONNECTION_TIMEOUT_MILLIS)));
         setClientForKey(VcClientKey.WAIT_FOR_UPDATE_CLIENT, _vcVlsi.connect(_vcCreds, useCert, _controlClient, 
               (customTimeout != null ? customTimeout : VC_WAIT_FOR_UPDATES_CONNECTION_TIMEOUT_MILLIS)));
         setClientForKey(VcClientKey.STATS_POLL_CLIENT, _vcVlsi.connect(_vcCreds, useCert, _controlClient, 
               (customTimeout != null ? customTimeout : VC_STATS_POLL_CONNECTION_TIMEOUT_MILLIS)));
         
         if ((_controlClient == null) || (_waitForUpdateClient == null) || (_statsPollClient == null)) {
            _log.log(Level.WARNING, "Unable to get VC client");
            return false;
         }
         return true;
      } catch (Exception e) {
         _log.warning("VHM: connection to vCenter failed ("+e.getClass()+"): "+e.getMessage());
         return false;
      }
   }

   private boolean connect(Long customTimeout) {
      boolean useCert = false;
      if ((_vcCreds.keyStoreFile != null) && (_vcCreds.keyStorePwd != null) && (_vcCreds.vcExtKey != null)) {
         useCert = true;
      }
      boolean success = initClients(useCert, customTimeout);
      if (useCert && !success && (_vcCreds.user != null) && (_vcCreds.password != null)) {
         _log.warning("VHM: certificate based login failed, trying with username and password");
         success = initClients(false, customTimeout);
      }
      if (!success) {
         _log.warning("VHM: could not obtain vCenter connection through any protocol");
         return false;
      }
      return _initialized = true;
   }

   /**
    * First tests the validity of a given connection and if it fails, attempts to reset all connections
    * 
    * @param client
    * @param customTimeout
    * @return
    */
   private boolean validateConnection(Client client, Long customTimeout) {
      boolean success = false;
      if (client != null) {
         success = _vcVlsi.testConnection(client);
      }
      if (!success) {
         /* Only one thread should win this race. That thread will initiate the connection and others will not be blocked waiting */
         if (_initiatingConnectionThread.compareAndSet(null, Thread.currentThread())) {
            if (_initialized) {
               _log.warning("VHM: connection to vCenter dropped, attempting reconnection");
            }
            success = connect(customTimeout);
            _initiatingConnectionThread.compareAndSet(Thread.currentThread(), null);
         } else {
            _log.info("VHM: VC undergoing initialization by other thread, so returning false");
            return false;
         }
      }
      return success;
   }
   
   private Client getAndValidate(VcClientKey clientKey, Long customTimeout) {
      if (validateConnection(getClientForKey(clientKey), customTimeout)) {
         _log.finer("Returning successfully validated client for "+clientKey);
         return getClientForKey(clientKey);
      }
      _log.finer("Returning null client for "+clientKey);
      return null;
   }

   /**
    * Returns a client for a particular task after validating that the connection is good
    * If the connection is not good, an attempt will be made to re-connect with VC
    * 
    * @param clientKey The VC Client
    * @return A valid client or null if a valid connection is not possible
    */
   protected Client getAndValidateClient(VcClientKey clientKey) {
      return getAndValidate(clientKey, null);
   }

   /**
    * Same as getAndValidateClient except that it has the side effect of resetting waitForUpdates state
    *   and can take a custom timeout to wait less time for the retry
    * 
    * @param clientKey The VC Client
    * @param customTimeout If not null, the custom timeout will be applied to the connection
    * @return A valid client or null if a valid connection is not possible
    */
   protected Client resetClient(VcClientKey clientKey, Long customTimeout) {
      if (clientKey.equals(VcClientKey.WAIT_FOR_UPDATE_CLIENT)) {
         _waitForUpdatesVersion = "";
      }
      return getAndValidate(clientKey, customTimeout);
   }
   
   /**
    * Returns the current waitForUpdates version used in the waitForUpdates client
    * The version is used as a stateful diff mechanism. Eg Block until there are any changes since version X
    * 
    * @return current waitForUpdates
    */
   protected String getWaitForUpdatesVersion() {
      return _waitForUpdatesVersion;
   }
   
   /**
    * Sets a new waitForUpdatesVersion following a successful waitForUpdates call
    */
   protected void updateWaitForUpdatesVersion(String waitForUpUpdatesVersion) {
      _waitForUpdatesVersion = waitForUpUpdatesVersion;
   }
}
