package com.vmware.vhadoop.api.vhm.strategy;

import java.util.Set;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

public interface VMChooser extends ClusterMapReader {
   /**
    * Selects VMs to enable from the specified cluster. The logic determining which VMs is provided by implementors.
    * Synonymous with chooseVMsToEnable(listVMsForCluster(clusterId), delta)
    * @param clusterId - the target cluster
    * @param delta - the number of VMs to enable
    * @return - set of VM ids to enable
    */
   Set<String> chooseVMsToEnable(String clusterId, int delta);

   /**
    * Selects VMs to disable from the specified cluster. The logic determining which VMs is provided by implementors.
    * Synonymous with chooseVMsToDisable(listVMsForCluster(clusterId), delta)
    * @param clusterId - the target cluster
    * @param delta - the number of VMs to disable
    * @return - set of VM ids to disable
    */
   Set<String> chooseVMsToDisable(String clusterId, int delta);

   /**
    * Selects VMs to enable from the specified set. The logic determining which VMs is provided by implementors.
    * @param candidates - the candidate VMs
    * @param delta - the number of VMs to enable
    * @return - set of VM ids to enable (subset of candidates)
    */
   Set<String> chooseVMsToEnable(Set<String> candidates, int delta);

   /**
    * Selects VMs to disable from the specified set. The logic determining which VMs is provided by implementors.
    * @param candidates - the candidate VMs
    * @param delta - the number of VMs to disable
    * @return - set of VM ids to disable (subset of candidates)
    */
   Set<String> chooseVMsToDisable(Set<String> candidates, int delta);

   /**
    * Selects a single VM out of the specified set to enable. All the candidates must be on the same host.
    * @param candidates - the candidate VMs
    * @return - set of VM ids to enable (subset of candidates)
    */
   String chooseVMToEnableOnHost(Set<String> candidates);

   /**
    * Selects a single VM out of the specified set to disable. All the candidates must be on the same host.
    * @param candidates - the candidate VMs
    * @return - set of VM ids to disable (subset of candidates)
    */
   String chooseVMToDisableOnHost(Set<String> candidates);
}
