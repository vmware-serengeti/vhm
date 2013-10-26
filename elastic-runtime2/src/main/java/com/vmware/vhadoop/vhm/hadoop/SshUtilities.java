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

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((username == null) ? 0 : username.hashCode());
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
         Credentials other = (Credentials) obj;
         if (username == null) {
            if (other.username != null)
               return false;
         } else if (!username.equals(other.username))
            return false;
         return true;
      }
   }

   public static final int DEFAULT_SSH_PORT = 22;

   int copy(String remote, int port, Credentials credentials, byte data[], String remoteDirectory, String remoteName, String permissions) throws IOException;
   int execute(String remote, int port, Credentials credentials, String command, OutputStream stdout) throws IOException;
   Process invoke(String remote, int port, Credentials credentials, String command, OutputStream stdout) throws IOException;
}
