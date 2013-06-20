/***************************************************************************
* Copyright (c) 2013 VMware, Inc. All Rights Reserved.
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

package com.vmware.vhadoop.adaptor.vc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManager;
import javax.xml.ws.soap.SOAPFaultException;

import com.vmware.vhadoop.adaptor.vc.VCConnection.MoRefAndProps;
import com.vmware.vhadoop.adaptor.vc.VCConnection.WaitProperty;
import com.vmware.vim25.ManagedObjectReference;

public class VCUtils {
   
   /* This is a utility class used for reducing SSL checking when testing */
   private static class TrustAllTrustManager implements javax.net.ssl.TrustManager, javax.net.ssl.X509TrustManager {

      public java.security.cert.X509Certificate[] getAcceptedIssuers() {
         return null;
      }

      public boolean isServerTrusted(java.security.cert.X509Certificate[] certs) {
         return true;
      }

      public boolean isClientTrusted(java.security.cert.X509Certificate[] certs) {
         return true;
      }

      public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType)
            throws java.security.cert.CertificateException {
         return;
      }

      public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType)
            throws java.security.cert.CertificateException {
         return;
      }
   }
   
   private static class VerifyAllHostNames implements HostnameVerifier {
      public boolean verify(String urlHostName, SSLSession session) {
         return true;
      }
   };

   /* 
    * 1) Trust all server certificates -- TODO verify server cert
    * 2) setup SSL to use given keystore to send client certificate. 
    */
   public static void trustAllHttpsCertificates(String keyStorePath, String keyStorePassword) {
      SSLContext sc;
      try {
         sc = SSLContext.getInstance("SSL");
         SSLSessionContext sslsc = sc.getServerSessionContext();
         sslsc.setSessionTimeout(0);

         KeyStore keyStore = KeyStore.getInstance("JKS");
         keyStore.load(new FileInputStream(keyStorePath), keyStorePassword.toCharArray());
         KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
         kmf.init(keyStore, keyStorePassword.toCharArray());
         sc.init(kmf.getKeyManagers(),
                 new TrustManager[]{(TrustManager)new TrustAllTrustManager()}, null);
         HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
         HttpsURLConnection.setDefaultHostnameVerifier(new VerifyAllHostNames());
      } catch (Exception e) {
         throw new RuntimeException("Unexpected HTTPS Exception", e);
      }
   }
   
   /* Ignore SSL certificates - useful for testing */
   public static void trustAllHttpsCertificates() {
      SSLContext sc;
      try {
         sc = SSLContext.getInstance("SSL");
         SSLSessionContext sslsc = sc.getServerSessionContext();
         sslsc.setSessionTimeout(0);
         sc.init(null, new TrustManager[]{(TrustManager)new TrustAllTrustManager()}, null);
         HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
         HttpsURLConnection.setDefaultHostnameVerifier(new VerifyAllHostNames());
      } catch (KeyManagementException e) {
         throw new RuntimeException("Unexpected HTTPS Exception", e);
      } catch (NoSuchAlgorithmException e) {
         throw new RuntimeException("Unexpected HTTPS Exception", e);
      }
   }

   public static String getSoapFaultExceptionMessage(SOAPFaultException sfe) {
      if (sfe.getFault().hasDetail()) {
          return sfe.getFault().getDetail().getFirstChild().getLocalName();
      }
      if (sfe.getFault().getFaultString() != null) {
         return "\n Message: " + sfe.getFault().getFaultString();
      }
      return sfe.getMessage();
   }
   
   /**
    * Transforms the result from a VC call into an asynchronous result, allowing us to 
    * return from the VC call before the call has actually completed.
    * This implementation integrates with the WaitProperty implementation so that you
    * can define properties which should have changed before the operation is considered complete
    * 
    */
   protected static abstract class ResultTransformer<DTOType> implements Future<DTOType> {
      ManagedObjectReference _waitForPropsOnObj;
      List<WaitProperty> _waitProperties;
      VCConnection _connection;
      
      public ResultTransformer(VCConnection connection) {
         _connection = connection;
      }

      /* The object we want to wait for property changes on */
      void setWaitForPropsOnObj(ManagedObjectReference waitForPropsOnObj) {
         _waitForPropsOnObj = waitForPropsOnObj;
      }
      
      /* Define the properties we want to look for changes in. 
       * Note that Future.get() will return if any one of these properties is met */
      void addWaitProperty(String filterProperty, String waitAttribute, Object[] expectedVals) {
         if (_waitProperties == null) {
            _waitProperties = new ArrayList<WaitProperty>();
         }
         _waitProperties.add(_connection.new WaitProperty(filterProperty, waitAttribute, expectedVals));
      }
      
      /* block and wait - returns success or failure */
      boolean waitForProps() {
         if ((_waitProperties != null) && (_waitForPropsOnObj != null)) {
            /* TODO: Consider some more complex success/failure criteria? */
            if (_connection.waitForPropertyChange(_waitForPropsOnObj, _waitProperties.toArray(new WaitProperty[0])) == null) {
               return false;
            }
         }
         return true;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         throw new RuntimeException("Not implemented!");
      }

      @Override
      public DTOType get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
         throw new RuntimeException("Not implemented!");
      }

      @Override
      public boolean isCancelled() {
         throw new RuntimeException("Not implemented!");
      }

      @Override
      public boolean isDone() {
         throw new RuntimeException("Not implemented!");
      }
   }
   
   /**
    * Simple non-waiting utility class for returning synchronous results with just a cast
    * 
    */
   protected static class CastingResultTransformer<DTOType> extends ResultTransformer<DTOType> {
      DTOType _toWrap;
      
      public CastingResultTransformer(DTOType toWrap) {
         super(null);
         _toWrap = toWrap;
      }
      
      @Override
      public DTOType get() {
         return _toWrap;
      }
   }

   /**
    * This is used for wrapping single DTO types returned from VC calls
    * The transform method should encapsulate the conversion to the particular DTO type
    * Note that the get() should block if any waits have been specified
    * 
    */
   protected static abstract class AsyncResultTransformer<DTOType> extends ResultTransformer<DTOType> {
      MoRefAndProps _toWrap;
      
      public AsyncResultTransformer(MoRefAndProps toWrap, VCConnection connection) {
         super(connection);
         _toWrap = toWrap;
      }

      public abstract DTOType transform(MoRefAndProps input);

      @Override
      public DTOType get() {
         if (waitForProps()) {
            return transform(_toWrap);
         }
         return null;
      }
   }
   
   /**
    * Same as AsyncResultTransformer for dealing with arrays of results
    * 
    */
   protected static abstract class AsyncArrayResultTransformer<External> extends ResultTransformer<External[]> {
      MoRefAndProps[] _toWrap;
      
      public AsyncArrayResultTransformer(MoRefAndProps[] toWrap, VCConnection connection) {
         super(connection);
         _toWrap = toWrap;
      }
      
      /* Called once for each element of the array */
      public abstract External transform(MoRefAndProps input);
      
      public abstract External[] toArray(List<External> results);

      public External[] get() {
         if (waitForProps()) {
            List<External> results = new ArrayList<External>();
            if (_toWrap == null) {
               return null;
            }
            for (MoRefAndProps moref : _toWrap) {
               results.add(transform(moref));
            }
            if (results.size() > 0) {
               return toArray(results);
            }
         }
         return null;
      }
   }

}
