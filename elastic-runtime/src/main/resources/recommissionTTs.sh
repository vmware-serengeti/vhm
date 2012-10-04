#!/bin/bash -x

# This script recommissions a list of tasktrackers from the jobtracker
# Prerequisites:
# * This script is deployed on the jobtracker VM
# * The file with the list of tasktrackers to be recommissioned is present and has
#   unique (no duplicate) entries
# * The jobtracker is running  
# * The excludes file (e.g., excludesTT) is specified in the conf/mapred-site.xml 
#   before starting the jobtracker
# 
# USAGE: $ME <RecommissionListFile> <ExcludesFile> <HadoopHome> 

# Constants
EXPECTED_ARGS=3
ME=`basename $0`
LOGFILE="$HOME/.$ME.log"
JTERRFILE="$HOME/.$ME.jt.stderr"
LOCKFILE="/var/lock/.derecommission.exclusiveLock" # Note: same LOCKFILE for de/recommission

# Errors/Warnings
ERROR_BAD_ARGS=100
ERROR_EXCLUDES_FILE_NOT_FOUND=101
ERROR_DRLIST_FILE_NOT_FOUND=102
ERROR_BAD_HADOOP_HOME=103
ERROR_JT_CONNECTION=104
ERROR_JT_UNKNOWN=105
ERROR_EXCLUDES_FILE_UPDATE=110
WARN_TT_EXCLUDESFILE=200
WARN_TT_ACTIVE=201

# Determine if the TT is active (running ok)

isActive()
{
    ttList=("${!1}")
    tt=tracker_"$2"
    
    for ttData in ${ttList[@]}; do	
	activeTT=`echo $ttData | cut -d: -f1`	
	if [ "$activeTT" = "$tt" ]; then
		return 1
	fi
    done

    return 0
}


# Parse the error file generated for JobTracker

parseJTErrFile()
{
    file="$1"
    connLine=`sed -n '11p' < $file`
#   lastLine=`tail -1 $file` # We could use this only for "hadoop mradmin"
    echo "$connLine"
    arr=( $connLine )
    lidx=${#arr[@]}
    if [[ "${arr[$((lidx-2))]}" = "Connection" && "${arr[$((lidx-1))]}" = "refused" ]]; then    
	echo "ERROR: Unable to connect to jobtracker"
	return $ERROR_JT_CONNECTION
    else
	echo "Unknown error related to jobtracker"
	return $ERROR_JT_UNKNOWN
    fi
}

# Check script arguments 

checkArguments()
{
    if [ $# -ne $EXPECTED_ARGS ]; then
	    echo "USAGE: $ME <RecommissionListFile> <ExcludesFile> <HadoopHome>" 
	    exit $ERROR_BAD_ARGS
    fi
    
    local loc_rListFile=$1
    local loc_excludesFile=$2
    local loc_hadoopHome=$3
    
    if [ ! -f $loc_rListFile ]; then
	echo "ERROR: Recommission list file \"$loc_rListFile\" not found" 
	exit $ERROR_DRLIST_FILE_NOT_FOUND
    fi
    
    if [ ! -f $loc_excludesFile ]; then
	echo "ERROR: Excludes file \"$loc_excludesFile\" not found" 
	exit $ERROR_EXCLUDES_FILE_NOT_FOUND
    fi
    
    if [ ! -f $loc_hadoopHome/bin/hadoop ]; then
	echo "ERROR: \"$loc_hadoopHome\" is not HADOOP_HOME" 
	exit $ERROR_BAD_HADOOP_HOME
    fi
}

# Exit uncermoniously

exitWithWarning()
{
    local loc_numToRecommission=$1
    local loc_missingTT=$2
    local loc_activeTT=$3
    
    if [[ $loc_missingTT -ge 1 ]]; then
	echo "WARNING: $loc_missingTT TTs were not in the excludes list" 
	echo "INFO: Successfully recommissioned $loc_numToRecommission TTs" 
	exit $WARN_TT_EXCLUDESFILE
    fi
	    
    if [[ $loc_activeTT -ge 1 ]]; then
	echo "WARNING: Tried to recommission $loc_activeTT active TTs" 
	echo "INFO: Successfully recommissioned $loc_numToRecommission TTs" 
	exit $WARN_TT_ACTIVE
    fi
}

main()
{
# Remove logfile if present
    rm -f $LOGFILE

# Redirect stdout to a log file
    exec > $LOGFILE

# Arguments check/set

    checkArguments $*

    rListFile=$1
    excludesFile=$2
    hadoopHome=$3
        
    echo "INFO: Arguments:: TT list to recommission: $rListFile; ExcludesFile: $excludesFile; hadoopHome: $hadoopHome" 
    
    missingTT=0
    flagPresent=0
    numActiveTT=0
    numToRecommission=0
    
# Ensure only one VHM executes this script at any given time
    
# Makes sure we exit if flock fails.
# set -e
    
    { 
# Wait for lock on $LOCKFILE (fd 200) for 10 seconds
	flock -x -w 10 200

# Generate list of active task trackers
	activeTTs=`$hadoopHome/bin/hadoop job -list-active-trackers 2> $JTERRFILE`
	
	if [ -s $JTERRFILE ]; then
	    parseJTErrFile $JTERRFILE
	    exit $?
	fi
	
	arrActiveTTs=( $activeTTs )
	
# Read and Update excludes file
# Assumption: excludesFile does not have duplicates
	excludesList=( `cat $excludesFile` )
	
	while read ttRecommission; do	    

	    for tt in ${excludesList[@]}; do
		if [ "$tt" = "$ttRecommission" ]; then
		    flagPresent=1
		    break
		fi
	    done 
	    
	    if [ $flagPresent -eq 1 ]; then
		flagPresent=0

		echo "INFO: Removing $ttRecommission from excludes file"
		sed -i "/$ttRecommission/d" $excludesFile
		returnVal=$?
		if [ $returnVal -ne 0 ]; then
			echo "ERROR: Error while trying to update excludes file"
			exit $ERROR_EXCLUDES_FILE_UPDATE
		fi
		isActive arrActiveTTs[@] $ttRecommission
		returnVal=$?
		if [ $returnVal -eq 0 ]; then
		    numToRecommission=$((numToRecommission+1))
		else
		    echo "WARNING: $ttRecommission is currently active!"
		    numActiveTT=$((numActiveTT+1))
		fi
	    else
		echo "WARNING: $tt is missing in the excludes file: $excludesFile" 
		missingTT=$((missingTT+1))
	    fi
	    
	done < $rListFile
	
	echo "INFO: Successfully updated excludes file. Latest excludes file: " 
	cat $excludesFile 
	
# Run recommission script by refreshing hosts
	$hadoopHome/bin/hadoop mradmin -refreshNodes 2> $JTERRFILE
	
	if [ -s $JTERRFILE ]; then
	    parseJTErrFile $JTERRFILE
	    exit $?
	fi
	
    } 200>$LOCKFILE

    exitWithWarning $numToRecommission $missingTT $numActiveTT 

    echo "INFO: Successfully recommissioned all $numToRecommission TTs in $rListFile" 
    exit 0	    
}

main $*