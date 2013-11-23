package com.vmware.vhadoop.vhm.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.vmware.vhadoop.util.ExternalizedParameters;

public class SshConnectionCache implements SshUtilities
{
   private static final Logger _log = Logger.getLogger(SshConnectionCache.class.getName());

   private static final int NUM_SSH_RETRIES = ExternalizedParameters.get().getInt("SSH_INITIAL_CONNECTION_NUMBER_OF_RETRIES");
   private static final int RETRY_DELAY_MILLIS = ExternalizedParameters.get().getInt("SSH_INITIAL_CONNECTION_RETRY_DELAY_MILLIS");
   private static final int INPUTSTREAM_TIMEOUT_MILLIS = ExternalizedParameters.get().getInt("SSH_REMOTE_EXECUTION_TIMEOUT_MILLIS");

   private static final String SCP_COMMAND = "scp  -t  ";

   static class Connection {

      final String hostname;
      final int port;
      final Credentials credentials;

      Connection(String hostname, int port, Credentials credentials) {
         this.hostname = hostname;
         this.port = port;
         this.credentials = credentials;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((credentials == null) ? 0 : credentials.hashCode());
         result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         Connection other = (Connection) obj;
         if (credentials == null) {
            if (other.credentials != null)
               return false;
         } else if (!credentials.equals(other.credentials))
            return false;
         if (hostname == null) {
            if (other.hostname != null)
               return false;
         } else if (!hostname.equals(other.hostname))
            return false;
         return true;
      }
   }

   class RemoteProcess extends Process {
      public final static int UNDEFINED_EXIT_STATUS = -1;

      Session session;
      ChannelExec channel;
      InputStream stdout;
      InputStream stderr;
      OutputStream stdin;

      public RemoteProcess(ChannelExec channel) throws IOException, JSchException {
         this.channel = channel;
         this.stdin = channel.getOutputStream();
         this.stderr = channel.getErrStream();
         this.stdout = channel.getInputStream();

         this.session = channel.getSession();
      }

      @Override
      protected void finalize() throws Throwable {
         super.finalize();
         cleanup();
      }

      @Override
      public OutputStream getOutputStream() {
         return stdin;
      }

      @Override
      public InputStream getInputStream() {
         return stdout;
      }
      
      @Override
      public InputStream getErrorStream() {
         return stderr;
      }

      @Override
      public int waitFor() throws InterruptedException {
         do {
            int status = channel.getExitStatus();
            if (status != -1) {
               cleanup();

               return status;
            }

            Thread.sleep(100);
         } while (true);
      }

      /**
       * Waits for the remote process to complete
       * @param timeout timeout in milliseconds
       * @return the return code for the process or -1 if the wait times out
       * @throws InterruptedException
       */
      public int waitFor(int timeout) throws InterruptedException {
         long deadline = System.currentTimeMillis() + timeout;

         do {
            int status = channel.getExitStatus();
            if (status != -1) {
               cleanup();

               return status;
            }

            Thread.sleep(100);
         } while (deadline > System.currentTimeMillis());

         return -1;
      }

      @Override
      public int exitValue() {
         return channel.getExitStatus();
      }

      @Override
      public void destroy() {
         try {
            channel.sendSignal("SIGKILL");
         } catch (Exception e) {
            _log.log(Level.INFO, "VHM: unable to send kill signal to remote process", e);
         }

         cleanup();
      }

      public synchronized void cleanup() {
         if (channel == null) {
            return;
         }

         channel.disconnect();
         synchronized (cache) {
            synchronized (channelMap) {
               Set<Channel> channels = channelMap.get(session);
               if (channels != null) {
                  channels.remove(channel);
                  if (channels.isEmpty() && !cache.containsValue(session)) {
                     channelMap.remove(session);
                     _log.fine("Disconnecting session during RemoteProcess cleanup for "+session.getUserName()+"@"+session.getHost());
                     session.disconnect();
                  }
               }
            }
         }

         channel = null;

         if (stdin != null) {
            try {
               stdin.close();
            } catch (IOException e) { /* squash */ }
         }

         if (stdout != null) {
            try {
               stdout.close();
            } catch (IOException e) { /* squash */ }
         }

         if (stderr != null) {
            try {
               stderr.close();
            } catch (IOException e) { /* squash */ }
         }
      }
   }

   private Map<Connection,Session> cache;
   private Map<Session,Set<Channel>> channelMap = new HashMap<Session,Set<Channel>>();

   private final JSch _jsch = new JSch();
   protected final int capacity;
   private float loadFactor = 0.75f;
   
 
   protected void clearCache() {
         synchronized (cache) {
            synchronized (channelMap) {
               for(Set<Channel> channelSet : channelMap.values()) {
                  /*TODO: need to disconnect channels if they are still connected?*/
                  channelSet.clear();
               }
               channelMap.clear();                  
            }
            
            for(Session session : cache.values()) {
               if(session.isConnected())
                  session.disconnect();
            }
            cache.clear();
         }
   }

   public SshConnectionCache(int capacity) {
      this.capacity = capacity;
      int baseSize = (int)Math.ceil(capacity / loadFactor) + 2;
      cache = Collections.synchronizedMap(new LinkedHashMap<Connection,Session>(baseSize, loadFactor, true) {
         private static final long serialVersionUID = 1328753943644428132L;

         @Override
         protected boolean removeEldestEntry (Map.Entry<Connection,Session> eldest) {
    
            boolean remove = size() > SshConnectionCache.this.capacity;
            /* if we're removing this session it has to be disconnected to avoid leaking sockets */
            if (remove) {
               Session session = eldest.getValue();
              
               synchronized (channelMap) {
                  _log.fine("Disconnecting session during cache eviction for "+session.getUserName()+"@"+session.getHost());
                  if (!channelMap.containsKey(session)) {
                     session.disconnect();
                  }
               }
            }

            return remove;
         }
      });
   }

   /**
    * Extension point method for child classes. The cache is unmodifiable but the elements in it are not.
    * Care should be taken when changing state of the contained objects.
    * @return an unmodifiable view of the cache
    */
   protected Map<Connection,Session> getCache() {
      return Collections.unmodifiableMap(cache);
   }

   /**
    * Create the basic JSCH session object that's going to be our handle to the host
    * @param connection
    * @return
    */
   protected Session createSession(Connection connection) {
      try {
         Session session = _jsch.getSession(connection.credentials.username, connection.hostname, connection.port);

         java.util.Properties config = new java.util.Properties();
         config.put("StrictHostKeyChecking", "no"); /* TODO: Necessary? Hack? Security hole?? */
         session.setConfig(config);

         session.setTimeout(15000);

         return session;
      } catch (JSchException e) {
         String msg = "VHM: "+connection.hostname+" - could not create ssh session container";
         _log.warning(msg + " - "+ e.getMessage());
         _log.log(Level.INFO, msg, e);
      }

      return null;
   }

   /**
    * Connect the provided session object using the credentials supplied.
    * @param session
    * @param credentials
    * @return
    */
   protected boolean connectSession(Session session, Credentials credentials) {
      if (session.isConnected()) {
         _log.finer("VHM: "+session.getHost()+" - using cached connection");
         return true;
      }

      for (int i = 0; i < NUM_SSH_RETRIES; i++) {
         try {
            // If private key file is specified and not already added, use that as identity
            String prvkeyFile = credentials.privateKeyFile;
            if (prvkeyFile != null && !_jsch.getIdentityNames().contains(prvkeyFile)) {
               _jsch.addIdentity(prvkeyFile);
            }

            if (credentials.password != null) {
               session.setPassword(credentials.password);
            }

            _log.finer("VHM: "+session.getHost()+" - establishing ssh connection");
            session.connect();
            return true;

         } catch (JSchException e) {
            _log.info("VHM: "+session.getHost()+" - could not create ssh channel to host - " + e.getMessage());
            if (i < NUM_SSH_RETRIES - 1) {
               try {
                  _log.info("VHM: "+session.getHost()+" - retrying ssh connection to host after delay");
                  Thread.sleep(RETRY_DELAY_MILLIS);
               } catch (InterruptedException e1) {
                  _log.info("VHM: unexpected interruption while waiting to retry ssh connection");
               }
            }
         }
      }

      _log.warning("VHM: "+session.getHost()+" - unable to establish ssh session");

      return false;
   }

   /**
    * Get the session to operate with for a given connection. This will return a connected cached session or create a new one.
    * @param connection
    * @return
    */
   protected Session getSession(Connection connection) {
      Session session = cache.get(connection);
      if (session == null) {
         session = createSession(connection);
         if (session == null) {
            return null;
         }
         cache.put(connection, session);
         channelMap.put(session, new HashSet<Channel>());
      }

      if (!connectSession(session, connection.credentials)) {
         cache.remove(connection);
         channelMap.remove(connection);
         if (session != null) {
            /* ensure that even if it's something odd causing connectSession to fail we clean up */
            session.disconnect();
         }
         session = null;
      }

      return session;
   }

   /**
    * This performs some validity checking on the streams from the SSH connection.
    * @param in
    * @return
    * @throws IOException
    */
   private boolean assertRemoteScpReady(InputStream in) throws IOException {
      int b = in.read();
      if (b == 0) {
         return true;
      } else if (b < 0) {
         _log.log(Level.INFO, "VHM: expected byte 0x0 but end of stream received");
         return false;
      } else {
         /* we weren't expecting data on this stream, so read it to log what we've been given */
         StringBuffer sb = new StringBuffer();
         do {
            sb.append((char) b);
            b = in.read();
         } while (b != '\n' && b >= 0);
         _log.log(Level.INFO, "VHM: expected byte 0x0 but saw the following data: " + sb.toString());
         return false;
      }
   }

   protected int copy(Connection connection, byte[] data, String remoteDirectory, String remoteName, String permissions) {
      int exitCode = RemoteProcess.UNDEFINED_EXIT_STATUS;
      String command = SCP_COMMAND + remoteDirectory + remoteName;
      RemoteProcess proc = null;

      try {
         proc = invoke(connection, command, null, null);

         OutputStream out = proc.getOutputStream();
         InputStream in = proc.getInputStream();

         if (!assertRemoteScpReady(in)) {
            _log.info("VHM: scp protocol error while preparing channel to remote host");
            return exitCode;
         }

         // send "C$perms filesize filename", where filename should not include
         StringBuilder params = new StringBuilder("C0").append(permissions);
         params.append(" ").append(data.length).append(" ");
         params.append(remoteName).append("\n");

         out.write(params.toString().getBytes());
         out.flush();

         if (!assertRemoteScpReady(in)) {
            _log.info("VHM: scp protocol error while waiting for confirmation of specified permissions for remote file");
            return exitCode;
         }

         out.write(data);
         out.write(new byte[] { 0 }, 0, 1);
         out.flush();

         if (!assertRemoteScpReady(in)) {
            _log.info("VHM: scp protocol error waiting for confirmation of data transfer");
         }

         out.close();
         /* set this explicitly here as that last assert provided us with the return code for the copy */
         exitCode = 0;
      } catch (Exception e) {
         String msg = "VHM: "+connection.hostname+" - exception copying data to remote host";
         _log.log(Level.WARNING, msg+" - "+e.getMessage());
         _log.log(Level.INFO, msg, e);

      } finally {
         if (proc != null) {
            proc.cleanup();
         }
      }

      return exitCode;
   }

   @Override
   public int copy(String remote, int port, Credentials credentials, byte[] data, String remoteDirectory, String remoteName, String permissions) {
      Connection connection = new Connection(remote, port, credentials);
      return copy(connection, data, remoteDirectory, remoteName, permissions);
   }

   protected int execute(Connection connection, String command, OutputStream stdout) throws IOException {
      int exitCode = RemoteProcess.UNDEFINED_EXIT_STATUS;
      RemoteProcess proc = null;

      try {
         proc = invoke(connection, command, stdout, null);
         long deadline = System.currentTimeMillis() + INPUTSTREAM_TIMEOUT_MILLIS;
         do {
            try {
               exitCode = proc.waitFor(INPUTSTREAM_TIMEOUT_MILLIS);
               if (exitCode != RemoteProcess.UNDEFINED_EXIT_STATUS) {
                  /* we only loop if the command hasn't completed */
                  break;
               }
            } catch (InterruptedException e) {
               _log.info("VHM: unexpected interruption while waiting for remote command to complete");
            }
         } while (deadline > System.currentTimeMillis());

         /* Caller is responsible for cleaning up resources passed in, but make sure all the data's been passed along */
         if (stdout != null) {
            try {
               stdout.flush();
            } catch (IOException e) { /* squash */ }
         }
      } catch (Exception e) {
         String msg = "VHM: "+connection.hostname+" - exception executing command on remote host";
         _log.log(Level.WARNING, msg+" - "+e.getMessage());
         _log.log(Level.INFO, msg, e);

      } finally {
         if (proc != null) {
            proc.cleanup();
         }
      }

      _log.log(Level.FINE, "Exit status from exec is: " + exitCode);
      return exitCode;
   }

   @Override
   public int execute(String remote, int port, Credentials credentials, String command, OutputStream stdout) throws IOException {
      Connection connection = new Connection(remote, port, credentials);
      return execute(connection, command, stdout);
   }

   public RemoteProcess invoke(Connection connection, String command, OutputStream stdout, InputStream stdin) throws IOException {
      /* get the cached session for the remote user/host or create a new one */
      ChannelExec channel;

      /* we synchronize on cache so that we don't have the potential to evict and disconnect a session in between confirming that
       * it's functional and recording an opening in the channel map */
      synchronized (cache) {
         synchronized (channelMap) {
            Session session = getSession(connection);
            if (session == null) {
               throw new IOException("unable to establish session to remote host "+connection.hostname);
            }

            /* open a new exec channel - this is tightly coupled to the execution of the command and will be closed on command completion */
            try {
               channel = (ChannelExec) session.openChannel("exec");
               Set<Channel> channels = channelMap.get(session);
               if (channels != null) {
                  channels.add(channel);
               }
            } catch (JSchException e) {
               String msg = "VHM: "+connection.hostname+" - exception opening SSH execution channel to host";
               _log.log(Level.INFO, msg, e);
               throw new IOException(msg);
            }
         }
      }

      /* execute the remote command and set up our remote process wrapper */
      RemoteProcess proc = null;
      try {
         _log.log(Level.FINE, "About to execute: " + command);

         if (command.startsWith("sudo")) {
            /* sudo requires an allocated pty */
            channel.setPty(true);
         }
         channel.setCommand(command);

         /* this calls getOutput/Error/InputStream which seems to overwrite anything set by setOutputStream, so needs to be done first */
         proc = new RemoteProcess(channel);

         /* if we have sink and source already, set the channels up */
         if (stdout != null) {
            channel.setOutputStream(stdout);
         }
         if (stdin != null) {
            channel.setInputStream(stdin);
         }

         channel.connect();

         /* if we have sink and source then the corresponding streams are already linked so shouldn't be read directly */
         if (stdout != null) {
            proc.stdout = null;
         }
         if (stdin != null) {
            proc.stdin = null;
         }

         _log.log(Level.FINE, "Finished channel connection in exec");

         return proc;
      } catch (Exception e) {
         String msg = "VHM: "+connection.hostname+" - exception invoking remote command on host";
         _log.log(Level.WARNING, msg+": "+e.getMessage());
         _log.log(Level.INFO, msg, e);

         channel.disconnect();

         if (proc != null) {
            proc.cleanup();
         }

         throw new IOException(msg);
      }
   }

   @Override
   public RemoteProcess invoke(String remote, int port, Credentials credentials, String command, OutputStream stdout) throws IOException {
      Connection connection = new Connection(remote, port, credentials);
      return invoke(connection, command, stdout, null);
   }
}
