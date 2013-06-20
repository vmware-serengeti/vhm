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

import static com.vmware.vhadoop.model.Resource.CPU;
import static com.vmware.vhadoop.model.Resource.MEMORY;

import java.util.Map;
import java.util.TreeMap;

abstract public class ResourceLimits implements Limits
{
   private Map<Resource,Object> limits;

   /**
    * Creates unlimited resource limits
    */
   ResourceLimits() {
      limits = new TreeMap<Resource,Object>();
      limits.put(MEMORY, Limits.UNLIMITED);
      limits.put(CPU, Limits.UNLIMITED);
   }

   /**
    * Returns the allocated memory in megabytes
    * @return
    */
   @Override
   public long getMemoryLimit() {
      return (Long)(limits.get(MEMORY));
   }

   /**
    * Returns the CPU allocation in Mhz
    * @return
    */
   @Override
   public long getCpuLimit() {
      return (Long)(limits.get(CPU));
   }

   /**
    * Sets the allocated memory in megabytes
    * @param allocation
    */
   @Override
   public void setMemoryLimit(long allocation) {
      limits.put(MEMORY, allocation);
   }

   /**
    * Sets the CPU allocation in Mhz
    * @param allocation
    */
   @Override
   public void setCpuLimit(long allocation) {
      limits.put(CPU, allocation);
   }
}