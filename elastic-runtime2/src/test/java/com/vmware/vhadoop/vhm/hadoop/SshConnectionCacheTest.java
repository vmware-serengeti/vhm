package com.vmware.vhadoop.vhm.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.jcraft.jsch.Session;
import com.vmware.vhadoop.vhm.hadoop.SshConnectionCache.Connection;
import com.vmware.vhadoop.vhm.hadoop.SshConnectionCache.RemoteProcess;
import com.vmware.vhadoop.vhm.hadoop.SshUtilities.Credentials;

public class SshConnectionCacheTest
{
   private static int MINIMUM_REQUIRED_HOST_ALIASES = 4;

//   private static String aliases[];
//   private static String username;*/
//   /* this will need to be set in the environment for the test. *//*
//   private static String password = System.getProperty("user.password");
//   private static String privateKeyFile;
//   private static String userHomePath;

   static File authorizedKeysBackup = null;
   static File authorizedKeysName = null;
   static File sshDir = null;

   TestSshConnectionCache cache;
   Credentials credentials;
   RemoteProcess remoteProc;

   //local test setting
   private static final String aliases[] = new String[] {"sshtest1", "sshtest2", "sshtest3", "sshtest4"};
   private static final String username = "ess";
   private static final String password = "ca$hc0w";
   private static final String privateKeyFile = "/home/ess/.ssh/id_rsa";
   private static final String userHomePath = "/home/"+username;

//   @AfterClass
//   public static void cleanup() throws IOException {
//      /* make sure that backup exists */
//      if (authorizedKeysBackup != null) {
//         Files.move(authorizedKeysBackup.toPath(), authorizedKeysName.toPath(), REPLACE_EXISTING);
//      }
//   }

//   @BeforeClass
//   public static void setup() throws IOException, InterruptedException {
//      /* we've got assumptions that we're on a unix distro, but go with linux */
//      Assume.assumeTrue("Test setup assumes certain things, which I've rolled up into - must be linux", "linux".equalsIgnoreCase(System.getProperty("os.name")));
//
//      username = System.getProperty("user.name");
//      assertNotNull("system property user.name", username);
//
//      userHomePath = System.getProperty("user.home");
//      assertNotNull("system property user.home", userHomePath);
//
//      sshDir = new File(userHomePath+"/.ssh");
//      File keyBase = File.createTempFile("sshtest", "", sshDir);
//      keyBase.deleteOnExit();
//
////      Process keygen = Runtime.getRuntime().exec("ssh-keygen -b 2048 -t rsa -N '' -C 'ssh test key' -f "+keyBase.getAbsolutePath());
//      Process keygen = Runtime.getRuntime().exec(new String[] {"ssh-keygen", "-q", "-b", "2048", "-t", "rsa", "-N", "", "-C", "ssh test key", "-f", keyBase.getAbsolutePath()});
//      /* shouldn't hurt to provide input for the overwrite prompt to overwrite the empty tempFile, -o not supported on some distros */
//      keygen.getOutputStream().write('y');
//      keygen.getOutputStream().write('\n');
//      keygen.getOutputStream().flush();
//
//      long deadline = System.currentTimeMillis() + 10000;
//      int ret = -1;
//      do {
//         try {
//            /* don't want to hang if this command doesn't return */
//            ret = keygen.exitValue();
//         } catch (IllegalThreadStateException e) { /* not finished yet */ }
//      } while (ret != 0 && System.currentTimeMillis() < deadline);
//
//      /* tidy up if it hung */
//      if (ret == -1) {
//         keygen.destroy();
//      }
//
//      BufferedReader stdout = new BufferedReader(new InputStreamReader(keygen.getInputStream()));
//      BufferedReader stderr = new BufferedReader(new InputStreamReader(keygen.getErrorStream()));
//
//      /* dump output for debugging */
//      for (String line = stdout.readLine(); line != null; line = stdout.readLine()) {
//         System.err.println(line);
//      }
//      for (String line = stderr.readLine(); line != null; line = stderr.readLine()) {
//         System.err.println(line);
//      }
//
//      assertEquals("keygen process didn't complete successfully", 0, ret);
//
//      /* add key to authorized hosts */
//      authorizedKeysName = new File(sshDir.getAbsolutePath()+"/authorized_keys");
//      authorizedKeysBackup = File.createTempFile("authorized_keys", "ssh_test_backup", sshDir);
//      try {
//         Files.copy(authorizedKeysName.toPath(), authorizedKeysBackup.toPath(), REPLACE_EXISTING);
//      } catch (java.nio.file.NoSuchFileException e) {
//         authorizedKeysBackup.delete();
//         authorizedKeysBackup = null;
//      }
//
//      File pub = new File(keyBase.getAbsoluteFile()+".pub");
//      String signature = Files.readAllLines(pub.toPath(), Charset.defaultCharset()).get(0);
//
//      OutputStreamWriter out = new FileWriter(authorizedKeysName, true);
//      out.write(signature);
//      out.flush();
//      out.close();
//
//      /* get the host aliases we have available */
//      List<String> hosts = Files.readAllLines(Paths.get("/etc/hosts"), Charset.defaultCharset());
//      for (String entry : hosts) {
//         /* expect ip name alias1 alias2 alias3 ... */
//         if (entry.charAt(0) == '#') {
//            continue;
//         }
//
//         if (entry.indexOf('#') != -1) {
//            entry = entry.substring(0, entry.indexOf('#'));
//         }
//
//         /* get rid of leading and trailing white space */
//         entry = entry.trim();
//
//         if (!entry.startsWith("127.0.0.1")) {
//            continue;
//         }
//
//         String details[] = entry.split("\\s");
//         assertTrue("expected at least "+MINIMUM_REQUIRED_HOST_ALIASES+" aliases in hosts entry", details.length > MINIMUM_REQUIRED_HOST_ALIASES);
//
//         aliases = Arrays.copyOfRange(details, 1, details.length);
//         break;
//      }
//   }

   class TestSshConnectionCache extends SshConnectionCache {

      public TestSshConnectionCache(int capacity) {
         super(capacity);
      }

      @Override
      protected Session createSession(Connection connection) {
         return super.createSession(connection);
      }

      @Override
      protected boolean connectSession(Session session, Credentials credentials) {
         return super.connectSession(session, credentials);
      }

      @Override
      protected Session getSession(Connection connection) {
         return super.getSession(connection);
      }

      @Override
      protected void clearCache() {
         super.clearCache();
      }

      void assertSessionCachedAndConnected(Connection connection) {
         if (connection == null)
            fail("connection object is null.");

         Map<Connection,Session> cache = getCache();
         Session session = cache.get(connection);

         if (session == null)
            fail("Session is not in the cache.");

         if (!session.isConnected())
            fail("Session is cached but not connected.");
      }

      void assertSessionCachedButDisconnected(Connection connection) {
         if (connection == null)
            fail("connection object is null.");

         Map<Connection,Session> cache = getCache();
         Session session = cache.get(connection);

         if (session == null)
            fail("Session is not in the cache.");

         if (session.isConnected())
            fail("Session is still connected.");
      }
      
      void dropConnectionInCache(Connection connection) {
         if (connection == null)
            fail("connection object is null.");

         Map<Connection,Session> cache = getCache();
         Session session = cache.get(connection);

         if (session == null)
            fail("Session is not in the cache.");
         
         session.disconnect();         
      }
      
      void assertSessionEvicted(Connection connection, Session session) {      
         if (connection == null)
            fail("argument: connection object is null.");

         if (session == null)
            fail("argument: session object is null.");

         Map<Connection,Session> cache = getCache();
         assertNull(cache.get(connection));
         assertTrue(!session.isConnected());
      }

      int getCacheSize() {
         return getCache().size();
      }
      
      void runInvokeNotReturn(Connection connection) throws IOException {
         String remoteDirectory =  System.getProperty("java.io.tmpdir");
         
         /*delete any file named 'invokeReturn' first*/
         remoteProc = invoke(connection, "cd " + remoteDirectory + "&& rm -f invokeReturn && while [ ! -f invokeReturn ]; do sleep 1; done", null, null);
         assertEquals(-1, remoteProc.exitValue());
      }
      
      /*make previous runInvokeNotReturn(.) return now*/
      void runInvokeResume(Connection connection) throws IOException, InterruptedException {
         if (connection == null)
            fail("connection object is null.");
         
         String remoteDirectory =  System.getProperty("java.io.tmpdir");
         String path = remoteDirectory + "/invokeReturn";
         File file = new File(path);
         file.createNewFile();
         
         /*ensure invoke returns from sleep*/
         Thread.sleep(2000);
        
         /*ensure not in cache*/
         Map<Connection,Session> cache = getCache();
         assertNull(cache.get(connection));
         
         /*previous invoke should return successfully and session should be disconnected.*/      
         assertNotNull(remoteProc);
         assertNotNull(remoteProc.session);
         assertTrue(remoteProc.exitValue() != -1);
         assertTrue(!remoteProc.session.isConnected());
         
         remoteProc = null;
      }
      
      void assertEvictedConnectionDoesntClose(Connection connection) {
         if (connection == null)
            fail("connection object is null.");

         Map<Connection,Session> cache = getCache();
         assertNull(cache.get(connection));
         
         /*channel is still in use*/
         assertNotNull(remoteProc);
         assertEquals(-1, remoteProc.exitValue());
         
         /*session does not close*/
         assertNotNull(remoteProc.session);
         assertTrue(remoteProc.session.isConnected());
      }
      
      void cleanupUnclosedEvictedConnection() {
         assertNotNull(remoteProc);
         remoteProc.destroy();
         remoteProc = null;
      }     
   }


   @Before
   public void populateCredentials() {
      credentials = new Credentials(username, password, privateKeyFile);
   }
   
   @After
   public void cacheCleanup() {
      cache.clearCache();
   }


   @Test
   /*basic sanity check, no cache operation*/
   public void connectionSanityCheck() throws IOException {
      cache = new TestSshConnectionCache(aliases.length);

      for (String alias : aliases) {
         Connection connection = new Connection(alias, SshUtilities.DEFAULT_SSH_PORT, credentials);
         Session session = cache.createSession(connection);

         assertNotNull(session);
         assertTrue(cache.connectSession(session, credentials));

         ByteArrayOutputStream out = new ByteArrayOutputStream();

         /* check if execute(.) works*/
         int returnVal = cache.execute(alias, SshUtilities.DEFAULT_SSH_PORT, credentials, "cd ~|pwd", out);
         assertEquals("Expected ssh'd cd+pwd command to return without error", 0, returnVal);

         /* '~' should be /home/username */
         assertEquals(userHomePath, out.toString().trim());

         /* check if copy(.) works*/
         String dataWritten = "test data";
         byte[] data = dataWritten.getBytes();
         String remoteDirectory =  System.getProperty("java.io.tmpdir") + "/";
         String remoteName = alias + ".dat";
         String permissions = "774";

         returnVal = cache.copy(alias, SshUtilities.DEFAULT_SSH_PORT, credentials, data, remoteDirectory, remoteName, permissions);
         assertEquals("Expected scp command to return without error", 0, returnVal);

         File testFile = new File(remoteDirectory + "/" + remoteName);
         assertTrue(testFile.exists());

         /*check data integrity*/
         out.reset();
         returnVal = cache.execute(alias, SshUtilities.DEFAULT_SSH_PORT, credentials, "cat "+ remoteDirectory + "/" + remoteName, out);

         assertEquals("Expected ssh'd cat command to return without error", 0, returnVal);
         assertTrue(out.toString().equals(dataWritten));
      }
   }


   @Test
   public void cachedConnectionIsReused() throws IOException {
      cache = new TestSshConnectionCache(aliases.length);
      assertEquals(0, cache.getCacheSize());

      /*initial connection with hosts*/
      for (String alias : aliases) {
         int returnVal = cache.execute(alias, SshUtilities.DEFAULT_SSH_PORT, credentials, "date", null);
         assertEquals("Expected ssh'd date command to return without error", 0, returnVal);
      }

      /*all sessions should get cached*/
      assertEquals(aliases.length, cache.getCacheSize());

      /*all sessions should be cached now and can be reused*/
      for (String alias : aliases) {
         Connection connection = new Connection(alias, SshUtilities.DEFAULT_SSH_PORT, credentials);
         cache.assertSessionCachedAndConnected(connection);

         /*reuse the session, exercise copy(.) and execute(.)*/
         String dataWritten = "test data";
         byte[] data = dataWritten.getBytes();
         String remoteDirectory =  System.getProperty("java.io.tmpdir") + "/";
         String remoteName = alias + ".dat";
         String permissions = "774";
         ByteArrayOutputStream out = new ByteArrayOutputStream();

         int returnVal = cache.copy(connection, data, remoteDirectory, remoteName, permissions);
         assertEquals("Expected scp command to return without error", 0, returnVal);

         File testFile = new File(remoteDirectory + "/" + remoteName);
         assertTrue(testFile.exists());

         /*check data integrity*/
         out.reset();
         returnVal = cache.execute(connection, "cat "+ remoteDirectory + "/" + remoteName, out);

         assertEquals("Expected ssh'd cat command to return without error", 0, returnVal);
         assertTrue(out.toString().equals(dataWritten));

         /*all sessions are still cached, no eviction happens in reuse*/
         assertEquals(aliases.length, cache.getCacheSize());
      }
   }


   @Test
   public void leastRecentlyUsedIsEvicted() throws IOException {
      int cacheCapacity = aliases.length/2;
      cache = new TestSshConnectionCache(cacheCapacity);
      assertEquals(0, cache.getCacheSize());

      int index;
      /*auxiliary data structure to record the order in which sessions are put into cache
        the head of the list is the eldest one currently in the cache*/
      List<Connection> connectionList = new LinkedList<Connection>();
      List<Session> sessionList = new LinkedList<Session>();

      /*populate the cache and make it full*/
      for(index = 0; index <cacheCapacity; index++) {
         Connection connection = new Connection(aliases[index], SshUtilities.DEFAULT_SSH_PORT, credentials);

         int returnVal = cache.execute(connection, "date", null);
         assertEquals("Expected ssh'd date command to return without error", 0, returnVal);

         /* new session should be in cache now*/
         cache.assertSessionCachedAndConnected(connection);

         /*record the order in which this session is put into cache*/
         connectionList.add(connection);
         sessionList.add(cache.getSession(connection));
      }

      assertEquals(cacheCapacity, cache.getCacheSize());

      /*From now on, each time a new session is put into cache, the eldest one is expected to be evicted*/
      for(index = cacheCapacity; index < aliases.length; index++) {
         Connection connection = new Connection(aliases[index], SshUtilities.DEFAULT_SSH_PORT, credentials);

         /*exercise copy(.) and execute(.) for the new session*/
         String dataWritten = "test data";
         byte[] data = dataWritten.getBytes();
         String remoteDirectory =  System.getProperty("java.io.tmpdir") + "/";
         String remoteName = aliases[index] + ".dat";
         String permissions = "774";
         ByteArrayOutputStream out = new ByteArrayOutputStream();

         int returnVal = cache.copy(connection, data, remoteDirectory, remoteName, permissions);
         assertEquals("Expected scp command to return without error", 0, returnVal);

         File testFile = new File(remoteDirectory + "/" + remoteName);
         assertTrue(testFile.exists());

         /*check data integrity*/
         out.reset();
         returnVal = cache.execute(connection, "cat "+ remoteDirectory + "/" + remoteName, out);

         assertEquals("Expected ssh'd cat command to return without error", 0, returnVal);
         assertTrue(out.toString().equals(dataWritten));

         assertEquals(cacheCapacity, cache.getCacheSize());

         /*new session should be in cache now*/
         cache.assertSessionCachedAndConnected(connection);

         /*record the order in which this session is put into cache*/
         connectionList.add(connection);
         sessionList.add(cache.getSession(connection));

         /*whether we have evicted the right (the eldest) Session (with index 0, head of the list) out of cache*/
         cache.assertSessionEvicted(connectionList.get(0), sessionList.get(0));

         connectionList.remove(0);
         sessionList.remove(0);
      }
   }


   @Test
   /*require the number of host aliases >= 4*/
   public void evictedConnectionIsRecreated() throws IOException {
      int cacheCapacity = aliases.length - 2;
      cache = new TestSshConnectionCache(cacheCapacity);
      assertEquals(0, cache.getCacheSize());

      int index;
      
      /*auxiliary data structure to record the order in which sessions are put into cache.
        the head of the list is the eldest one currently in the cache.*/
      List<Connection> connectionList = new LinkedList<Connection>();
      List<Session> sessionList = new LinkedList<Session>();
      
      /*connections evicted*/
      List<Connection> evictedConnectionList = new LinkedList<Connection>();

      /*populate the cache. When the last two sessions come into cache,
        the first two sessions are expected to be evicted*/
      for(index = 0; index < aliases.length; index++) {
         Connection connection = new Connection(aliases[index], SshUtilities.DEFAULT_SSH_PORT, credentials);

         int returnVal = cache.execute(connection, "date", null);
         assertEquals("Expected ssh'd date command to return without error", 0, returnVal);

         /* new session should be in cache now*/
         cache.assertSessionCachedAndConnected(connection);

         /*record the order in which this session is put into cache*/
         connectionList.add(connection);
         sessionList.add(cache.getSession(connection));
         
         /*cache size should not change when it gets full, since the eldest one will be evicted when a new one comes*/
         if(index >= cacheCapacity - 1) {
            assertEquals(cacheCapacity, cache.getCacheSize());
         }
         
         /*eldest session is kicked out*/
         if(index >= cacheCapacity) {
            cache.assertSessionEvicted(connectionList.get(0), sessionList.get(0));
            
            evictedConnectionList.add(connectionList.get(0));
            connectionList.remove(0);
            sessionList.remove(0);
         }
      }

      /*re-visit evicted connections, exercise execute(.) and copy(.)*/
      for(index = 0; index < 2; index++) {
         if(index == 0) {
            int returnVal = cache.execute(evictedConnectionList.get(index), "date", null);
            assertEquals("Expected ssh'd date command to return without error", 0, returnVal);            
         } else {
            String dataWritten = "test data";
            byte[] data = dataWritten.getBytes();
            String remoteDirectory =  System.getProperty("java.io.tmpdir") + "/";
            String remoteName = evictedConnectionList.get(index).hostname + ".dat";
            String permissions = "774";

            int returnVal = cache.copy(evictedConnectionList.get(index), data, remoteDirectory, remoteName, permissions);
            assertEquals("Expected scp command to return without error", 0, returnVal);

            File testFile = new File(remoteDirectory + "/" + remoteName);
            assertTrue(testFile.exists());            
         }
         
         /*evicted connections should be recreated and put into cache again*/
         assertEquals(cacheCapacity, cache.getCacheSize());
         cache.assertSessionCachedAndConnected(evictedConnectionList.get(index));
         
         /*ensure the session currently with index 0 has been evicted*/
         cache.assertSessionEvicted(connectionList.get(0), sessionList.get(0));         
         connectionList.remove(0);
         sessionList.remove(0);
      }
   }

   
   @Test
   public void previouslyEvictedCachedConnectionIsReused() throws IOException {
      int cacheCapacity = aliases.length - 1;
      cache = new TestSshConnectionCache(cacheCapacity);
      assertEquals(0, cache.getCacheSize());

      int index;
      
      /*auxiliary data structure to record the order in which sessions are put into cache.
        the head of the list is the eldest one currently in the cache.*/
      List<Connection> connectionList = new LinkedList<Connection>();
      List<Session> sessionList = new LinkedList<Session>();
      
      /*populate the cache. When the last session comes into cache,
        the first one is expected to be evicted*/
      for(index = 0; index < aliases.length; index++) {
         Connection connection = new Connection(aliases[index], SshUtilities.DEFAULT_SSH_PORT, credentials);

         int returnVal = cache.execute(connection, "date", null);
         assertEquals("Expected ssh'd date command to return without error", 0, returnVal);

         /* new session should be in cache now*/
         cache.assertSessionCachedAndConnected(connection);

         /*record the order in which this session is put into cache*/
         connectionList.add(connection);
         sessionList.add(cache.getSession(connection));
         
         /*cache size should not change when it gets full, since the eldest one will be evicted when a new one comes*/
         if(index >= cacheCapacity - 1) {
            assertEquals(cacheCapacity, cache.getCacheSize());
         }
      }

      /*ensure the eldest session is kicked out*/
      Connection eldestConnection = connectionList.get(0);
      cache.assertSessionEvicted(connectionList.get(0), sessionList.get(0));
      connectionList.remove(0);
      sessionList.remove(0);
      
      /*re-visit previously evicted connection*/ 
      int returnVal = cache.execute(eldestConnection, "date", null);
      assertEquals("Expected ssh'd date command to return without error", 0, returnVal); 
      
      /*previously evicted connection should be recreated and put into cache again*/
      assertEquals(cacheCapacity, cache.getCacheSize());
      cache.assertSessionCachedAndConnected(eldestConnection);
      
      /*ensure the session currently with index 0 has been evicted*/
      cache.assertSessionEvicted(connectionList.get(0), sessionList.get(0));         

      /*reuse that "eldest connection"*/
      String dataWritten = "test data";
      byte[] data = dataWritten.getBytes();
      String remoteDirectory =  System.getProperty("java.io.tmpdir") + "/";
      String remoteName = eldestConnection.hostname + ".dat";
      String permissions = "774";
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      returnVal = cache.copy(eldestConnection, data, remoteDirectory, remoteName, permissions);
      assertEquals("Expected scp command to return without error", 0, returnVal);

      File testFile = new File(remoteDirectory + "/" + remoteName);
      assertTrue(testFile.exists());

      /*check data integrity*/
      out.reset();
      returnVal = cache.execute(eldestConnection, "cat "+ remoteDirectory + "/" + remoteName, out);

      assertEquals("Expected ssh'd cat command to return without error", 0, returnVal);
      assertTrue(out.toString().equals(dataWritten));

      assertEquals(cacheCapacity, cache.getCacheSize());
   }

   
   @Test
   public void evictedConnectionDoesntCloseWhileInUse() throws IOException {
      int cacheCapacity = 1;
      cache = new TestSshConnectionCache(cacheCapacity);
      assertEquals(0, cache.getCacheSize());
      
      /*create a session in use*/
      Connection firstConnection = new Connection(aliases[0], SshUtilities.DEFAULT_SSH_PORT, credentials);
      cache.runInvokeNotReturn(firstConnection);
      
      assertEquals(1, cache.getCacheSize());
      cache.assertSessionCachedAndConnected(firstConnection);
      
      /*evict firstConnection*/
      Connection secondConnection = new Connection(aliases[1], SshUtilities.DEFAULT_SSH_PORT, credentials);
      
      int returnVal = cache.execute(secondConnection, "date", null);
      assertEquals("Expected ssh'd date command to return without error", 0, returnVal);

      /* secondConnection should be in cache now*/
      cache.assertSessionCachedAndConnected(secondConnection);
      assertEquals(1, cache.getCacheSize());
      
      /*ensure firstConnection is evicted but does not close while it's still in use */
      cache.assertEvictedConnectionDoesntClose(firstConnection);
      cache.cleanupUnclosedEvictedConnection();
   }


   @Test
   public void evictedConnectionCloseAfterUse() throws IOException, InterruptedException {
      int cacheCapacity = 1;
      cache = new TestSshConnectionCache(cacheCapacity);
      assertEquals(0, cache.getCacheSize());
      
      /*create a session in use*/
      Connection firstConnection = new Connection(aliases[0], SshUtilities.DEFAULT_SSH_PORT, credentials);
      cache.runInvokeNotReturn(firstConnection);
      
      assertEquals(1, cache.getCacheSize());
      cache.assertSessionCachedAndConnected(firstConnection);
      
      /*evict firstConnection*/
      Connection secondConnection = new Connection(aliases[1], SshUtilities.DEFAULT_SSH_PORT, credentials);
      
      int returnVal = cache.execute(secondConnection, "date", null);
      assertEquals("Expected ssh'd date command to return without error", 0, returnVal);

      /* secondConnection should be in cache now*/
      cache.assertSessionCachedAndConnected(secondConnection);
      assertEquals(1, cache.getCacheSize());
      
      /*ensure firstConnection is evicted but does not close while it's still in use */
      cache.assertEvictedConnectionDoesntClose(firstConnection);
      
      /*close firstConnection which is evicted*/
      cache.runInvokeResume(firstConnection);
   }

   
   @Test
   public void cachedDroppedConnectionIsRecreated() throws IOException {
      int cacheCapacity = 2;
      cache = new TestSshConnectionCache(cacheCapacity);
      assertEquals(0, cache.getCacheSize());
      
      Connection[] connection = new Connection[cacheCapacity];

      /*populate the cache*/
      for(int index = 0; index < cacheCapacity; index++) {
         connection[index] = new Connection(aliases[index], SshUtilities.DEFAULT_SSH_PORT, credentials);

         int returnVal = cache.execute(connection[index], "date", null);
         assertEquals("Expected ssh'd date command to return without error", 0, returnVal);
         
         assertEquals(index + 1, cache.getCacheSize());         
      }

      /*two sessions are cached and connected*/
      for(int index = 0; index < cacheCapacity; index++) {
         cache.assertSessionCachedAndConnected(connection[index]);
      }
      
      /*drop both connections*/
      for(int index = 0; index < cacheCapacity; index++) {
         cache.dropConnectionInCache(connection[index]);
      }
      
      /*two sessions are still cached but dropped*/
      assertEquals(cacheCapacity, cache.getCacheSize());
      
      for(int index = 0; index < cacheCapacity; index++) {
         cache.assertSessionCachedButDisconnected(connection[index]);
      }
      
      /*exercise execute(.) with the first dropped connection*/
      int returnVal = cache.execute(connection[0], "date", null);
      assertEquals("Expected ssh'd date command to return without error", 0, returnVal);
      
      assertEquals(cacheCapacity, cache.getCacheSize());         
      
      /*exercise copy(.) with the second dropped connection*/
      String dataWritten = "test data";
      byte[] data = dataWritten.getBytes();
      String remoteDirectory =  System.getProperty("java.io.tmpdir") + "/";
      String remoteName = aliases[1] + ".dat";
      String permissions = "774";
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      returnVal = cache.copy(connection[1], data, remoteDirectory, remoteName, permissions);
      assertEquals("Expected scp command to return without error", 0, returnVal);

      File testFile = new File(remoteDirectory + "/" + remoteName);
      assertTrue(testFile.exists());

      /*check data integrity*/
      out.reset();
      returnVal = cache.execute(connection[1], "cat "+ remoteDirectory + "/" + remoteName, out);

      assertEquals("Expected ssh'd cat command to return without error", 0, returnVal);
      assertTrue(out.toString().equals(dataWritten));
      
      assertEquals(cacheCapacity, cache.getCacheSize());  
      
      /*two sessions are cached and connected now*/
      for(int index = 0; index < cacheCapacity; index++) {
         cache.assertSessionCachedAndConnected(connection[index]);
      }
   }
}
