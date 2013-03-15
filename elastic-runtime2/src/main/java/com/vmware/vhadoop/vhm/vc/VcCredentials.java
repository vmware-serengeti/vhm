package com.vmware.vhadoop.vhm.vc;

// can authenticate either using user/password or cert/keystore
public class VcCredentials {
   public String vcIP;
   public String vcThumbprint;

   public String user;
   public String password;

   public String keyStoreFile;
   public String keyStorePwd;
   public String vcExtKey;
}
