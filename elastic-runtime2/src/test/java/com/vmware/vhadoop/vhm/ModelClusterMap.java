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

package com.vmware.vhadoop.vhm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public class ModelClusterMap implements ClusterMap
{
	protected final static boolean ON = true;
	protected final static boolean OFF = false;

	class VM
	{
		String id;
		String cluster;
		String host;
		boolean power;

		public VM(final String id, final String cluster, final String host, final boolean power) {
			this.id = id;
			this.cluster = cluster;
			this.host = host;
			this.power = power;
		}

		@Override
      public String toString() {
		   StringBuffer sb = new StringBuffer();

		   sb.append("VM[id: ").append(id).append(", cluster:").append(cluster).append(", host:").append(host).append(", power: ").append(power);

		   return sb.toString();
		}
	}

	List<VM> vms = new LinkedList<VM>();

	public void populateTestData() {
      vms.add(new VM("vm1", "clusterA", "hostX", ON));
      vms.add(new VM("vm2", "clusterA", "hostY", ON));
      vms.add(new VM("vm3", "clusterA", "hostX", ON));
      vms.add(new VM("vm4", "clusterA", "hostY", OFF));
      vms.add(new VM("vm5", "clusterB", "hostX", ON));
      vms.add(new VM("vm6", "clusterB", "hostY", ON));
      vms.add(new VM("vm7", "clusterB", "hostX", ON));
      vms.add(new VM("vm8", "clusterB", "hostY", OFF));
	}

	public ModelClusterMap(boolean prepopulate) {
	   if (prepopulate) {
	      populateTestData();
	   }
	}

	public List<VM> getMapContents() {
	   return vms;
	}

	public void clearMap() {
	   vms.clear();
	}

	public void addVMToMap(final String name, final String cluster, final String host, final boolean power) {
	   vms.add(new VM(name, cluster, host, power));
	}

	public boolean setPowerStateForVM(final String name, final boolean power) {
	   for (VM vm : vms) {
	      if (vm.id.equals(name)) {
	         boolean result = vm.power;
	         vm.power = power;
	         return result;
	      }
	   }

	   return false;
	}

	@Override
	public Set<String> listComputeVMsForClusterAndPowerState(final String clusterId, final boolean powerState) {
		Set<String> selected = new HashSet<String>();
		for (VM vm : vms) {
			if (vm.cluster.equals(clusterId) && vm.power == powerState) {
				selected.add(vm.id);
			}
		}

		return selected;
	}

	@Override
	public Set<String> listComputeVMsForClusterHostAndPowerState(final String clusterId, final String hostId, final boolean powerState) {
		Set<String> selected = new HashSet<String>();
		for (VM vm : vms) {
			if (vm.cluster.equals(clusterId) && vm.power == powerState && vm.host.equals(hostId)) {
				selected.add(vm.id);
			}
		}

		return selected;
	}

	@Override
	public Set<String> listComputeVMsForPowerState(final boolean powerState) {
		Set<String> selected = new HashSet<String>();
		for (VM vm : vms) {
			if (vm.power == powerState) {
				selected.add(vm.id);
			}
		}

		return selected;
	}

	@Override
	public Set<String> listHostsWithComputeVMsForCluster(final String clusterId) {
		Set<String> selected = new HashSet<String>();
		for (VM vm : vms) {
			selected.add(vm.host);
		}

		return selected;
	}

	@Override
	public String getClusterIdForFolder(final String clusterFolderName) {
		return null;
	}

	@Override
	public String getHostIdForVm(final String vmid) {
		for (VM vm : vms) {
			if (vm.id.equals(vmid)) {
				return vm.host;
			}
		}

		return null;
	}

	@Override
	public Map<String, String> getHostIdsForVMs(final Set<String> vmsToED) {
		Map<String, String> selected = new HashMap<String, String>();
		for (VM vm : vms) {
			if (vmsToED.contains(vm.id)) {
				selected.put(vm.id, vm.host);
			}
		}

		return selected;
	}

	@Override
	public ClusterScaleCompletionEvent getLastClusterScaleCompletionEvent(final String clusterId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean checkPowerStateOfVms(final Set<String> vmIds, final boolean expectedPowerState) {
		for (VM vm : vms) {
			if (vmIds.contains(vm.id) && expectedPowerState != vm.power) {
				return false;
			}
		}

		return true;
	}

	@Override
	public Set<String> getAllKnownClusterIds() {
		Set<String> ids = new HashSet<String>();
		for (VM vm : vms) {
			ids.add(vm.cluster);
		}

		return ids;
	}

	@Override
	public HadoopClusterInfo getHadoopInfoForCluster(final String clusterId) {
		// TODO Auto-generated method stub
		return null;
	}

   @Override
   public Map<String, String> getDnsNamesForVMs(final Set<String> vms) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public String getClusterIdForVm(final String vm) {
      for (VM target : vms) {
         if (target.id.equals(vm)) {
            return target.cluster;
         }
      }

      return null;
   }

   @Override
   public String toString() {
      StringBuffer sb = new StringBuffer(super.toString()+":\n");

      for (VM vm : vms) {
         sb.append(vm).append("\n");
      }

      return sb.toString();
   }

   @Override
   public Integer getNumVCPUsForVm(String vm) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public Long getPowerOnTimeForVm(String vm) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public String getScaleStrategyKey(String clusterId) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public String getExtraInfo(String clusterId, String key) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public Set<String> getAllClusterIdsForScaleStrategyKey(String key) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public Set<String> listComputeVMsForCluster(String clusterId) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public Map<String, String> getVmIdsForDnsNames(Set<String> dnsNames) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public Boolean checkPowerStateOfVm(String vmId, boolean expectedPowerState) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public String getDnsNameForVM(String vmId) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public String getVmIdForDnsName(String dnsName) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public Map<String, Set<String>> getNicAndIpAddressesForVm(String vmId) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public Long getPowerOffTimeForVm(String vmId) {
      // TODO Auto-generated method stub
      return null;
   }

   @Override
   public String getMasterVmIdForCluster(String clusterId) {
      // TODO Auto-generated method stub
      return null;
   }
}
