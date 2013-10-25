package com.vmware.vhadoop.vhm.hadoop;

import java.io.IOException;
import java.io.OutputStream;

public interface SshUtilities
{
   class Credentials {
      String username;
      String password;
      String privateKeyFile;

      public Credentials(String username, String password, String privateKeyFile) {
         this.username = username;
         this.password = password;
         this.privateKeyFile = privateKeyFile;
      }

      boolean equals() {
         return username.equals(username);
      }
   }

   int copy(String remote, Credentials credentials, byte data[], String remoteDirectory, String remoteName, String permissions) throws IOException;
   int copy(String remote, int port, Credentials credentials, byte data[], String remoteDirectory, String remoteName, String permissions) throws IOException;

   int execute(String remote, Credentials credentials, String command, OutputStream stdout) throws IOException;
   int execute(String remote, int port, Credentials credentials, String command, OutputStream stdout) throws IOException;

   Process invoke(String remote, Credentials credentials, String command) throws IOException;
   Process invoke(String remote, int port, Credentials credentials, String command, OutputStream stdout) throws IOException;
}
