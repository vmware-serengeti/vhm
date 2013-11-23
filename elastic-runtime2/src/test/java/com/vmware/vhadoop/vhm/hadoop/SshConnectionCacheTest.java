package com.vmware.vhadoop.vhm.hadoop;

import static org.junit.Assert.*;

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
import com.vmware.vhadoop.vhm.hadoop.SshUtilities.Credentials;

public class SshConnectionCacheTest
{
   private static final String aliases[] = new String[] {"sshtest1", "sshtest2", "sshtest3", "sshtest4"};
   private static final String username = "ess";
   private static final String password = "ca$hc0w";
   private static final String privateKeyFile = "/home/ess/.ssh/id_rsa";
   
   private static final String userHomePath = "/home/"+username;
   
   TestSshConnectionCache cache;
   Credentials credentials;
   
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

         Map<Connection,Session> cache = getCache();
         
         if (connection == null) {
            fail("connection object is null.");
         } else {
            Session session = cache.get(connection);

            if (session == null)
               fail("Session is not in the cache.");
            
            if (!session.isConnected())
               fail("Session is cached but not connected.");
         }
      }
      
      void assertSessionEvicted(Connection connection, Session session) {

         Map<Connection,Session> cache = getCache();
         
         if (connection == null) {
            fail("argument: connection object is null.");
            
         } else if (session == null) {
            fail("argument: session object is null.");
            
         } else {
            assertNull(cache.get(connection));
            assertTrue(!session.isConnected());
         }
      }      
      
      int getCacheSize() {
         return getCache().size();
      }
   }
   

   @Before
   public void populateCredentials() {
      credentials = new Credentials(username, password, privateKeyFile);
   }
 
   
   @Test
   /*basic sanity check, no cache operation*/
   public void connectionSanityCheck() throws IOException {
      
      TestSshConnectionCache cache = new TestSshConnectionCache(aliases.length);

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
      TestSshConnectionCache cache = new TestSshConnectionCache(aliases.length);      
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
      TestSshConnectionCache cache = new TestSshConnectionCache(cacheCapacity);      
      assertEquals(0, cache.getCacheSize());
      
      int index;
      /*auxiliary data structure to record the order in which sessions are put into cache*/
      List<Connection> connectionList = new LinkedList<Connection>();
      List<Session> sessionList = new LinkedList<Session>();
      
      /*populate the cache and make it full*/
      for(index = 0; index <cacheCapacity; index++) {
         Connection connection = new Connection(aliases[index], SshUtilities.DEFAULT_SSH_PORT, credentials);
         
         int returnVal = cache.execute(connection, "date", null);         
         assertEquals("Expected ssh'd date command to return without error", 0, returnVal);
         
         /*session should be in cache now*/
         cache.assertSessionCachedAndConnected(connection);
 
         /*record the order in which this session is put into cache*/
         Session session = cache.getSession(connection);
         connectionList.add(connection);
         sessionList.add(session);
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
         Session session = cache.getSession(connection);
         connectionList.add(connection);
         sessionList.add(session);
         
         /*whether we have evicted the right (the eldest) Session (with index 0, head of the list) out of cache*/
         cache.assertSessionEvicted(connectionList.get(0), sessionList.get(0));
         
         connectionList.remove(0);
         sessionList.remove(0);
      }
   }
   

   @Ignore
   @Test
   public void evictedConnectionIsRecreated() {

   }

   @Ignore
   @Test
   public void previouslyEvictedCachedConnectionIsReused() {

   }

   @Ignore
   @Test
   public void evictedConnectionDoesntCloseWhileInUse() {

   }

   @Ignore
   @Test
   public void cachedDroppedConnectionIsRecreated() {

   }
}
