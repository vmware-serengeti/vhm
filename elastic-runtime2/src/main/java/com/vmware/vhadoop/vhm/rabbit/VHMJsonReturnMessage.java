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

package com.vmware.vhadoop.vhm.rabbit;

import com.google.gson.Gson;

public class VHMJsonReturnMessage {
  
   // Serengeti status update interface JSON packet definition  
   Boolean finished;
   Boolean succeed;
   int progress;
   int error_code;
   String error_msg;
   String progress_msg;

   
   public VHMJsonReturnMessage(
          Boolean param_finished,
          Boolean param_succeed,
          int param_progress,
          int param_error_code,
          String param_error_msg,
          String param_progress_msg) {
      finished = param_finished;
      succeed = param_succeed;
      progress = param_progress;
      error_code = param_error_code;
      error_msg = param_error_msg;
      progress_msg = param_progress_msg;
   }
   
   public byte[] getRawPayload() {
      Gson gson = new Gson();
      return gson.toJson(this).getBytes();
   }

}
