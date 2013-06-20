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

package com.vmware.vhadoop.model;

public interface Limits
{
   public static final long UNLIMITED = -1;
   public static final long IO_RATE_MBS = 300;

   /**
    * Returns the allocated memory in megabytes
    * @return
    */
   public long getMemoryLimit();

   /**
    * Returns the CPU allocation in Mhz
    * @return
    */
   public long getCpuLimit();

   /**
    * Sets the allocated memory in megabytes
    * @param allocation
    */
   void setMemoryLimit(long allocation);

   /**
    * Sets the CPU allocation in Mhz
    * @param allocation
    */
   void setCpuLimit(long allocation);
}