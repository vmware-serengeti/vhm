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

package com.vmware.vhadoop.model.scenarios;

import com.vmware.vhadoop.model.Workload;

public class ConstantWorkload extends Workload
{
   /** Default proportion of active/committed memory */
   private final static float DEFAULT_ACTIVE_PROPORTION = 0.7f;

   long cpu;
   long memory;

   /**
    * Creates a constant workload with the specified cpu and memory characteristics
    * @param cpu
    * @param memory
    */
   public ConstantWorkload(long cpu, long memory) {
      super("Constant workload");
      this.cpu = cpu;
      this.memory = memory;
   }

   /**
    * Starts the workload, starts using resources
    */
   @Override
   public void start() {
      setCpuUsage(cpu);
      setMemoryUsage(memory);
   }

   /**
    * Provides basic assumption that a given proportion of the memory usage is active at any given point
    * @return the active memory in Mb
    */
   @Override
   public long getActiveMemory() {
      return (long) (getMemoryUsage() * DEFAULT_ACTIVE_PROPORTION);
   }
}
