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

package com.vmware.vhadoop.vhm.hadoop;

import com.vmware.vhadoop.vhm.hadoop.HadoopConnection.HadoopCredentials;

public class SimpleHadoopCredentials implements HadoopCredentials {

   private final String _sshUsername;
   private final String _sshPassword;
   private final String _sshPrvkeyFile;

   public SimpleHadoopCredentials(String sshUsername, String sshPassword, String sshPrvkeyFile) {
      _sshUsername = sshUsername;
      _sshPassword = sshPassword;
      _sshPrvkeyFile = sshPrvkeyFile;
   }

   @Override
   public String getSshUsername() {
      return _sshUsername;
   }

   @Override
   public String getSshPassword() {
      return _sshPassword;
   }

   @Override
   public String getSshPrvkeyFile() {
      return _sshPrvkeyFile;
   }
}
