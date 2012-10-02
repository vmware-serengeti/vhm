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

import java.util.concurrent.Future;

import com.vmware.vhadoop.external.ExternalHostActions;
import com.vmware.vhadoop.external.OVF;
import com.vmware.vhadoop.external.VCActionDTOTypes.ClusterDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.DataCenterDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.FolderDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.HostDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.ResourcePoolDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VCDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMPowerState;
import com.vmware.vhadoop.external.VCActions;

/**
 * This class implements every method in the VCActions interface with an exception
 * Those methods which we want to implement in VCAdaptor over-ride the ones in here
 * Having more methods in VCActions than we need allows us to develop experimental code
 *
 */
public abstract class AbstractVCAdaptor implements VCActions {

   @Override
   public String getVMHostname(VMDTO vm) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public ExternalHostActions getExternalHostActions(HostDTO host) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO> getVMForName(String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<HostDTO> getHostForName(String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<ClusterDTO> getClusterForName(String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<DataCenterDTO> getDataCenterForName(String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<FolderDTO> getFolderForName(FolderDTO rootFolder, String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<FolderDTO> getRootFolder() {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<ResourcePoolDTO> getResourcePoolForName(String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO> createVMOnHost(OVF ovf, String name, HostDTO host) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO> createVMOnCluster(OVF ovf, String name, ClusterDTO cluster) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<ResourcePoolDTO> createResourcePool(String name, long memLimit) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<DataCenterDTO> createDataCenter(String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<ClusterDTO> createCluster(DataCenterDTO dataCenter, String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<FolderDTO> createFolder(FolderDTO parent, String name) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<HostDTO> addHostToCluster(ClusterDTO cluster, HostDTO host) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VCDTO> addVCObjectToFolder(FolderDTO folder, VCDTO objectToAdd) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO[]> listVMsOnHost(HostDTO host) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO[]> listVMsInFolder(FolderDTO folder) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO[]> listVMsInResourcePool(ResourcePoolDTO resourcePool) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<HostDTO[]> listHostsInCluster(ClusterDTO hosts) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<ClusterDTO> getClusterForHost(HostDTO host, boolean refresh) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<DataCenterDTO> getDataCenterForCluster(ClusterDTO cluster, boolean refresh) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<ResourcePoolDTO> getResourcePoolForVM(VMDTO vm, boolean refresh) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO> migrateVM(VMDTO vm, ResourcePoolDTO resourcePool) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO> migrateVM(VMDTO vm, HostDTO host) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO[]> powerOnVM(DataCenterDTO dataCenter, VMDTO[] vms) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO> powerOnVM(VMDTO vm) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMDTO> powerOffVM(VMDTO vm) {
      throw new RuntimeException("Unimplemented!");
   }
   
   @Override
   public Future<VMDTO> shutdownGuest(VMDTO vm) {
	  throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<VMPowerState> getPowerState(VMDTO vm, boolean refresh) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<Object> getDataInVC(String key) {
      throw new RuntimeException("Unimplemented!");
   }

   @Override
   public Future<Object> putDataInVC(String key, Object value) {
      throw new RuntimeException("Unimplemented!");
   }

}
