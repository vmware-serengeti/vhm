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

import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.SUCCESS;
import static com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.UNKNOWN_ERROR;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import com.vmware.vhadoop.adaptor.hadoop.HadoopConnection.HadoopCredentials;
import com.vmware.vhadoop.adaptor.hadoop.HadoopConnection.SshUtils;

/**
 * An implementation of SshUtils defined by HadoopConnection
 * This is marked clearly as being not thread-safe, 
 * so instances of this class should not be shared by multiple HadoopConnections
 *
 */
public class NonThreadSafeSshUtils implements SshUtils {

   private JSch _jsch = new JSch();

   private static final String SCP_COMMAND = "scp  -t  ";
   private static final int INPUTSTREAM_TIMEOUT = 100;
   
   @Override
   public ChannelExec createChannel(Logger logger, final HadoopCredentials credentials, String host, int port) {
      try {
    	 
         Session session = _jsch.getSession(credentials.getSshUsername(), host, port);

         session.setPassword(credentials.getSshPassword());
         UserInfo ui = new SSHUserInfo(credentials.getSshPassword());
         session.setUserInfo(ui);


         java.util.Properties config = new java.util.Properties();
         config.put("StrictHostKeyChecking", "no");        /* TODO: Necessary? Hack? Security hole?? */
         session.setConfig(config);
        
         // JG: Adding session timeout...
         session.setTimeout(15000);
        
         session.connect();
        
         return (ChannelExec)session.openChannel("exec");
      } catch (JSchException e) {
         logger.log(Level.SEVERE, "Could not create channel (e.g., wrong username/password) on host "+host, e);
         return null;
      }
   }

   @Override
   public int exec(Logger logger, ChannelExec channel, OutputStream out, String command) {
      int exitStatus = UNKNOWN_ERROR;
      try {
         logger.log(Level.FINE, "About to execute: "+command);   	  
         channel.setCommand(command);
         channel.setOutputStream(out);
         channel.setInputStream(null);         /* TODO: Why? */
         InputStream in = channel.getInputStream();
         channel.connect();

        logger.log(Level.FINE, "Finished channel connection in exec");

         if (!testChannel(logger, channel)) {
            return UNKNOWN_ERROR;          /* TODO: Improve */
         }
         
         int numSecs = 0;
         byte[] tmp = new byte[1024];
         while (true) {
            while (in.available() > 0) {
               int i = in.read(tmp, 0, 1024);
               if (i < 0)
                  break;
               out.write(tmp);
            }
            if (channel.isClosed()) {
               exitStatus = channel.getExitStatus();
               logger.log(Level.SEVERE, "Exit status from exec is: " + channel.getExitStatus());
               break;
            }
            
            try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.log(Level.SEVERE, "Excpetion while sleeping in exec");
			}
            
            numSecs++;
            
            if (numSecs == INPUTSTREAM_TIMEOUT) {
            	logger.log(Level.SEVERE, "No input was received for " + numSecs + "s while running remote exec!");
            	break;
            }
         }
      } catch (JSchException e) {
         logger.log(Level.SEVERE, "Unexpected JSch exception executing over SSH", e);
      } catch (IOException e) {
         logger.log(Level.SEVERE, "Unexpected IOException executing over SSH", e);
      }
      
      logger.log(Level.FINE, "Exit status from exec is: " + exitStatus);
      return exitStatus;
   }

   @Override
   public int scpBytes(Logger logger, ChannelExec channel, byte[] data, 
         String remotePath, String remoteFileName, String perms) {
      try {
         channel.setCommand(SCP_COMMAND + remotePath + remoteFileName);

         OutputStream out = channel.getOutputStream();
         InputStream in = channel.getInputStream();
     
         channel.connect();

         if (!testChannel(logger, channel)) {
            return UNKNOWN_ERROR;          /* TODO: Improve */
         }

         if (!waitForInputStream(logger, in)) {
            return UNKNOWN_ERROR;          /* TODO: Improve */
         }
         
         // send "C$perms filesize filename", where filename should not include
         StringBuilder params = new StringBuilder("C0").append(perms);
         params.append(" ").append(data.length).append(" ");
         params.append(remoteFileName).append("\n");
    
         out.write(params.toString().getBytes());
         out.flush();
         
         if (!waitForInputStream(logger, in)) {
            cleanup(logger, out, channel);
            logger.log(Level.SEVERE, "Error before writing SCP bytes");
            return UNKNOWN_ERROR;           /* TODO: Improve */
         }
         
         out.write(data);
         out.write(new byte[]{0}, 0, 1);
         out.flush();
         
         if (!waitForInputStream(logger, in)) {
            logger.log(Level.SEVERE, "Error after writing SCP bytes");
            cleanup(logger, out, channel);
            return -1;
         }
         
         out.close();
      } catch (Exception e) {
         logger.log(Level.SEVERE, "Unexpected Exception copying data to Job Tracker", e);
      }
      return SUCCESS;
   }

   private boolean waitForInputStream(Logger log, InputStream in) throws IOException {
      int b = in.read();
      if (b <= 0) {
         boolean result = (b == 0);
         if (!result) {
            log.log(Level.SEVERE, "SSH Channel failed test. First byte returned < 0 result");
         }
         return result;
      } else {
         StringBuffer sb = new StringBuffer();
         int c;
         do {
            c = in.read();
            sb.append((char) c);
         } while (c != '\n');
         log.log(Level.SEVERE, "SSH Channel failed test. Msg="+sb.toString());
         return false;
      }
   }
   
   @Override
   public boolean testChannel(Logger log, ChannelExec channel) {
      if (channel == null) {
         return false;
      }
      if (channel.isConnected()) {
         return true;
      }
      try {
         channel.connect();
      } catch (JSchException e) {
         log.log(Level.SEVERE, "SSH Channel failed test. Could not connect", e);
      }
      return channel.isConnected();
   }

   @Override
   public void cleanup(Logger log, OutputStream out, ChannelExec channel) {
      Session session = null;
      if (out != null) {
         try {
            out.flush();
            out.close();
         } catch (IOException e) {
            log.log(Level.WARNING, "Unexpected exception in SSH OutputStream cleanup", e);
         }
      }
      if (channel != null && channel.isConnected()) {
         try {
            session = channel.getSession();
         } catch (JSchException e) {
            log.log(Level.WARNING, "Unexpected exception in SSH channel cleanup", e);
         }
         channel.disconnect();
      }
      if (session != null && session.isConnected()) {
         session.disconnect();
      }
   }

   private class SSHUserInfo implements UserInfo {
      String _password;

      public SSHUserInfo(String password) {
         _password = password;
      }

      @Override
      public String getPassword() {
         return _password;
      }

      @Override
      public String getPassphrase() {
         return null;
      }

      @Override
      public boolean promptPassphrase(String arg0) {
         return false;
      }

      @Override
      public boolean promptPassword(String arg0) {
         return false;
      }

      @Override
      public boolean promptYesNo(String arg0) {
         return false;
      }

      @Override
      public void showMessage(String arg0) {
         System.out.println(arg0);
      }
   }
}
