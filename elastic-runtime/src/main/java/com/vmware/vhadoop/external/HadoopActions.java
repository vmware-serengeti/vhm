package com.vmware.vhadoop.external;

public interface HadoopActions {

   /** 
	 * 	Decommission a given set of TaskTrackers from a JobTracker 
	 *	@param TaskTrackers that need to be decommissioned
	 *  @return SUCCESS/FAIL
	 */
		public boolean decommissionTTs(String[] tts, HadoopCluster cluster);

	/** 
	 * 	Recommission a given set of TaskTrackers to a JobTracker 
	 *	@param TaskTrackers that need to be recommissioned/added
	 *  @return SUCCESS/FAIL
	 */
		public boolean recommissionTTs(String[] tts, HadoopCluster cluster);

		
		public boolean checkTargetTTsSuccess(String opType, String[] tts, int totalTargetEnabled, HadoopCluster cluster);
}
