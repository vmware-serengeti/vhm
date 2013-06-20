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

package com.vmware.vhadoop.vhm;

public class VHMSimpleInputMessage implements EmbeddedVHM.VHMInputMessage {
   
   private byte[] _data;
   private String _clusterName;
   private int _targetTTs;
   private String _jobTracker;
   private String[] _ttFolderNames;
   private String _rootFolder;
   
   public VHMSimpleInputMessage(byte[] data) {
      _data = data;
      String stringData = new String(data);
      String delims = "[ ]+";
      String[] tokens = stringData.split(delims);
      _clusterName = tokens[0];
      _targetTTs = Integer.parseInt(tokens[2]);
      _jobTracker = tokens[3];
      _ttFolderNames = tokens[4].split(",");
      _rootFolder = tokens[5];
   }

   @Override
   public byte[] getRawPayload() {
      return _data;
   }
   
   @Override
   public String getClusterName() {
      return _clusterName;
   }
   
   @Override
   public int getTargetTTs() {
      return _targetTTs;
   }

   @Override
   public String getJobTrackerAddress() {
      return _jobTracker;
   }

   @Override
   public String[] getTTFolderNames() {
      return _ttFolderNames;
   }

   @Override
   public String getSerengetiRootFolder() {
      return _rootFolder;
   }

}
