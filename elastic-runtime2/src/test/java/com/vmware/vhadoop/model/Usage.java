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


public interface Usage
{
   /**
    * Gets the maximum stable interval for the container and it's children. Should be overridden by
    * inheritors who don't have stable resource usage patterns.
    * @return Long.MAX_VALUE
    */
   public long getStableInterval();

   /**
    * Returns the allocated memory in megabytes
    * @return consumed memory in Mb
    */
   public long getMemoryUsage();

   /**
    * Returns the memory that's actively being used at a given point
    * @return active memory in Mb
    */
   public long getActiveMemory();

   /**
    * Returns the CPU allocation in Mhz
    * @return cpu usage in Mhz
    */
   public long getCpuUsage();

   /**
    * Adds a parent container to be notified when our usage changes
    */
   public void addParent(ResourceContainer parent);

   /**
    * Removes a parent container from the list
    */
   public void removeParent(ResourceContainer parent);
}