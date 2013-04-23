package com.vmware.vhadoop.vhm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMap;
import com.vmware.vhadoop.api.vhm.ClusterMapReader.ClusterMapAccess;
import com.vmware.vhadoop.api.vhm.HadoopActions.HadoopClusterInfo;
import com.vmware.vhadoop.api.vhm.events.ClusterScaleCompletionEvent;

public class StandaloneSimpleClusterMap extends AbstractClusterMapReader implements ClusterMap, ClusterMapAccess
{
	protected final static boolean ON = true;
	protected final static boolean OFF = false;

	private Object _clusterMapWriteLock;

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
	}

	List<VM> vms = new LinkedList<VM>();

	public StandaloneSimpleClusterMap() {
		vms.add(new VM("vm1", "clusterA", "hostX", ON));
		vms.add(new VM("vm2", "clusterA", "hostY", ON));
		vms.add(new VM("vm3", "clusterA", "hostX", ON));
		vms.add(new VM("vm4", "clusterA", "hostY", OFF));
		vms.add(new VM("vm5", "clusterB", "hostX", ON));
		vms.add(new VM("vm6", "clusterB", "hostY", ON));
		vms.add(new VM("vm7", "clusterB", "hostX", ON));
		vms.add(new VM("vm8", "clusterB", "hostY", OFF));
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
	public boolean checkPowerStateOfVms(final Set<String> vmIds, final boolean expectedPowerState) {
		for (VM vm : vms) {
			if (vmIds.contains(vm.id) && expectedPowerState != vm.power) {
				return false;
			}
		}

		return true;
	}

	@Override
	public String[] getAllKnownClusterIds() {
		Set<String> ids = new HashSet<String>();
		for (VM vm : vms) {
			ids.add(vm.cluster);
		}

		return ids.toArray(new String[] {});
	}

	@Override
	public String getScaleStrategyKey(final String clusterId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HadoopClusterInfo getHadoopInfoForCluster(final String clusterId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClusterMapAccess clone() {
		return this;
	}

	@Override
	public ClusterMap lockClusterMap() {
		return this;
	}

	@Override
	public void unlockClusterMap(final ClusterMap clusterMap) {
		if (clusterMap != this) {
			throw new RuntimeException("Trying to unlock wrong cluster map!");
		}
	}

   @Override
   public Set<String> getDnsNameForVMs(final Set<String> vms) {
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
}
