package com.vmware.vhadoop.adaptor.hadoop;

import com.vmware.vhadoop.adaptor.hadoop.HadoopConnection.HadoopCredentials;

public class SimpleHadoopCredentials implements HadoopCredentials {

   private String _sshUsername;
   private String _sshPassword;

   public SimpleHadoopCredentials(String sshUsername, String sshPassword) {
      _sshUsername = sshUsername;
      _sshPassword = sshPassword;
   }

   @Override
   public String getSshUsername() {
      return _sshUsername;
   }

   @Override
   public String getSshPassword() {
      return _sshPassword;
   }

}
