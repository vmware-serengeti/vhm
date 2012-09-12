package com.vmware.vhadoop.adaptor.vc;

import com.vmware.vhadoop.adaptor.vc.VCConnection.VCCredentials;

public class SimpleVCCredentials implements VCCredentials {

   String _hostName;
   String _userName;
   String _password;

   public SimpleVCCredentials(String hostName, String userName, String password) {
      _hostName = hostName;
      _userName = userName;
      _password = password;
   }
      
   @Override
   public String getHostName() {
      return _hostName;
   }

   @Override
   public String getUserName() {
      return _userName;
   }

   @Override
   public String getPassword() {
      return _password;
   }

}
