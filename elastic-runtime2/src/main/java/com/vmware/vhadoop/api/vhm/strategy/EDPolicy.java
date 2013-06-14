package com.vmware.vhadoop.api.vhm.strategy;

import java.util.*;

import com.vmware.vhadoop.api.vhm.ClusterMapReader;

/* Takes a set of VMs and either enables or disables them, based on whatever strategy it needs */
public interface EDPolicy extends ClusterMapReader {
   /* Caller should expect this to block - returns the VM IDs that were successfully enabled */
   Set<String> enableTTs(Set<String> toEnable, int totalTargetEnabled, String clusterId) throws Exception;

   /* Caller should expect this to block - returns the VM IDs that were successfully disabled */
   Set<String> disableTTs(Set<String> toDisable, int totalTargetEnabled, String clusterId) throws Exception;
   
   Set<String> getActiveTTs(String clusterId) throws Exception;
}
