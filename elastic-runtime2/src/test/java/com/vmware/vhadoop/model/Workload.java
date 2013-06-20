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

abstract public class Workload extends ResourceUsage
{
   /**
    * Basic constructor that takes the workload ID for later use
    * @param id
    */
   public Workload(String id) {
      super(id);
   }

   /**
    * Stops the workload
    * @param b - force stop if true, shutdown cleanly if false
    */
   public void stop(boolean b) {
      setCpuUsage(0);
      setMemoryUsage(0);
   }

   /**
    * Starts the workload running. This commences resource utilization.
    */
   public abstract void start();

}
