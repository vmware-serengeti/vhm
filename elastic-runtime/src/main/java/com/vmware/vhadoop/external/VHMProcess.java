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

package com.vmware.vhadoop.external;

/* New VHM can operate as a component of a VM, but is not in and of itself a VM - allows for embedding */
public abstract class VHMProcess implements GuestProcess<Object, Object> /* TODO: Too generic */ {

   private String _name;
   private long _memUsed;
   private boolean _modelInit;
   
   public void setModelInitParams(String name, long memUsed) {
      _name = name;
      _memUsed = memUsed;
      _modelInit = true;
   }

   @Override
   public void start(ProcessDeathCallback callback) {
      if (!_modelInit) {
         throw new RuntimeException("Model VHM not initialized");
      }
   }

   @Override
   public int getPID() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public long getMemUsed() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public boolean isHung() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public boolean shutdown() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public void kill() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public String getName() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public Object execute(Object o) {
      throw new RuntimeException("Not implemented");
   }

}
