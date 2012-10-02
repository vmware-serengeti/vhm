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
