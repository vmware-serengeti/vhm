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

import com.vmware.vhadoop.adaptor.vc.VCConnection.VCCredentials;

public class SecureVCCredentials implements VCCredentials {

   String _hostName;
   String _extensionKey;

   public SecureVCCredentials(String hostName, String extensionKey) {
      _hostName = hostName;
      _extensionKey = extensionKey;
   }
      
   @Override
   public String getHostName() {
      return _hostName;
   }

   @Override
   public String getExtensionKey() {
      return _extensionKey;
   }

   @Override
   public String getUserName() {
      return null;
   }

   @Override
   public String getPassword() {
      return null;
   }
   
}
