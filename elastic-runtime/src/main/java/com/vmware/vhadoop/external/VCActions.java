package com.vmware.vhadoop.external;

import java.util.concurrent.Future;

import com.vmware.vhadoop.external.VCActionDTOTypes.ClusterDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.DataCenterDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.FolderDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.HostDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.ResourcePoolDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VCDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMPowerState;

public interface VCActions {

   public boolean testConnection();
   
   public void dropConnection();  // Used for testing

   public String getVMHostname(VMDTO vm);

   /* Any operations on a host via VC should be done through this ExternalHostActions API */
   public ExternalHostActions getExternalHostActions(HostDTO host);

   public Future<VMDTO> getVMForName(String name);

   public Future<HostDTO> getHostForName(String name);

   public Future<ClusterDTO> getClusterForName(String name);

   public Future<DataCenterDTO> getDataCenterForName(String name);

   public Future<FolderDTO> getFolderForName(FolderDTO rootFolder, String name);

   public Future<FolderDTO> getRootFolder();

   public Future<ResourcePoolDTO> getResourcePoolForName(String name);

   public Future<VMDTO> createVMOnHost(OVF ovf, String name, HostDTO host);

   /* Picks the host in the cluster with the most memory */
   public Future<VMDTO> createVMOnCluster(OVF ovf, String name, ClusterDTO cluster);

   public Future<ResourcePoolDTO> createResourcePool(String name, long memLimit);

   public Future<DataCenterDTO> createDataCenter(String name);

   public Future<ClusterDTO> createCluster(DataCenterDTO dataCenter, String name);

   public Future<FolderDTO> createFolder(FolderDTO parent, String name);

   public Future<HostDTO> addHostToCluster(ClusterDTO cluster, HostDTO host);

   public Future<VCDTO> addVCObjectToFolder(FolderDTO folder, VCDTO objectToAdd);

   public Future<VMDTO[]> listVMsOnHost(HostDTO host);

   public Future<VMDTO[]> listVMsInFolder(FolderDTO folder);

   public Future<VMDTO[]> listVMsInResourcePool(ResourcePoolDTO resourcePool);

   public Future<HostDTO[]> listHostsInCluster(ClusterDTO hosts);

   public Future<HostDTO> getHostForVM(VMDTO vm, boolean refresh);

   public Future<ClusterDTO> getClusterForHost(HostDTO host, boolean refresh);

   public Future<DataCenterDTO> getDataCenterForCluster(ClusterDTO cluster, boolean refresh);

   public Future<ResourcePoolDTO> getResourcePoolForVM(VMDTO vm, boolean refresh);

   /* resourcePool can be null */
   public Future<VMDTO> migrateVM(VMDTO vm, ResourcePoolDTO resourcePool);

   public Future<VMDTO> migrateVM(VMDTO vm, HostDTO host);

   public Future<VMDTO[]> powerOnVM(DataCenterDTO dataCenter, VMDTO[] vms);
   
   public Future<VMDTO> powerOnVM(VMDTO vm);

   public Future<VMDTO> powerOffVM(VMDTO vm);
   
   public Future<VMDTO> shutdownGuest(VMDTO vm);

   public Future<VMPowerState> getPowerState(VMDTO vm, boolean refresh);

   public Future<Object> getDataInVC(String key);

   public Future<Object> putDataInVC(String key, Object value);
}
