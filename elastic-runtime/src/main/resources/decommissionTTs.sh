#!/bin/bash -x

# This script decommissions a list of tasktrackers from the jobtracker
# Prerequisites:
# * This script is deployed on the jobtracker VM
# * The file with the list of tasktrackers to be decommissioned is present and has
#   unique (no duplicate) entries
# * The jobtracker is running  
# * The excludes file (e.g., excludesTT) is specified in the conf/mapred-site.xml 
#   before starting the jobtracker
# 
# USAGE: $ME <DecommissionListFile> <ExcludesFile> <HadoopHome> 

# Constants
EXPECTED_ARGS=3
ME=`basename $0`
LOGFILE="$HOME/.$ME.log"
JTERRFILE="$HOME/.$ME.jt.stderr"
LOCKFILE="/var/lock/.derecommission.exclusiveLock" # Note: same LOCKFILE for de/recommission

# Errors/Warnings
ERROR_BAD_ARGS=100
ERROR_EXCLUDES_FILE_NOT_FOUND=101
ERROR_DLIST_FILE_NOT_FOUND=102
ERROR_BAD_HADOOP_HOME=103
ERROR_JT_CONNECTION=104
ERROR_JT_UNKNOWN=105
ERROR_FAIL_DECOMMISSION=106
ERROR_EXCLUDES_FILE_UPDATE=110
ERROR_LOCK_FILE_WRITE=111
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
	    echo "USAGE: $ME <DecommissionListFile> <ExcludesFile> <HadoopHome>" 
	    exit $ERROR_BAD_ARGS
    fi
    
    local loc_dListFile=$1
    local loc_excludesFile=$2
    local loc_hadoopHome=$3
    
    if [ ! -f $loc_dListFile ]; then
	echo "ERROR: Decommission list file \"$loc_dListFile\" not found" 
	exit $ERROR_DLIST_FILE_NOT_FOUND
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

# Get number of decommissions that failed

getNumFailDecommission()
{
    local loc_numActiveTTs=$1
    local loc_numToDecommission=$2
    local loc_hadoopHome=$3


    local loc_numFailDecommission=0
    local loc_ctr=0

    while true; do
	newActiveTTs=`$loc_hadoopHome/bin/hadoop job -list-active-trackers 2> $JTERRFILE`
	arrNewActiveTTs=( $newActiveTTs )
	if [ -s $JTERRFILE ]; then
	    parseJTErrFile $JTERRFILE
	    exit $?
	fi
	
	numNewActiveTTs=${#arrNewActiveTTs[@]}
	numDecommissioned=$((loc_numActiveTTs-numNewActiveTTs))
	
	if [[ $numDecommissioned -ne $loc_numToDecommission ]]; then
	    loc_ctr=$((loc_ctr+1))
	    if [[ $loc_ctr -eq 10 ]]; then
		loc_numFailDecommission=$((loc_numToDecommission-numDecommissioned))
		break
	    else
		echo "Waiting # $loc_ctr"
		sleep 1
	    fi
	else
	    break
	fi
    done
    
    return $loc_numFailDecommission
}

# Exit uncermoniously

exitWithWarningOrError()
{
    local loc_numFailDecommission=$1
    local loc_numToDecommission=$2
    local loc_dupl=$3
    local loc_inactiveTT=$4
	local loc_lockExitVal=$5


    if [[ $loc_numFailDecommission -ge 1 ]]; then
		echo "ERROR: Failed to decommission $loc_numFailDecommission out of $loc_numToDecommission TTs"
		exit $ERROR_FAIL_DECOMMISSION
    fi
    
    if [[ $loc_dupl -ge 1 ]]; then
		echo "WARNING: $loc_dupl TTs were already in the excludes list" 
		echo "INFO: Successfully decommissioned $loc_numToDecommission TTs" 
		exit $WARN_TT_EXCLUDESFILE
    fi
	    
    if [[ $loc_inactiveTT -ge 1 ]]; then
		echo "WARNING: Tried to decommission $loc_inactiveTT inactive TTs" 
		echo "INFO: Successfully decommissioned $loc_numToDecommission TTs" 
	exit $WARN_TT_ACTIVE
    fi
	
    if [[ $loc_lockExitVal -ne 0 ]]; then
		echo "ERROR: Failed to write to lock file $LOCKFILE (permissions problem?)"
		exit $ERROR_LOCK_FILE_WRITE
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

    dListFile=$1
    excludesFile=$2
    hadoopHome=$3
        
    echo "INFO: Arguments:: TT list to decommission: $dListFile; ExcludesFile: $excludesFile; hadoopHome: $hadoopHome" 
    
    dupl=0
    flagDupl=0
    inactiveTT=0
    numToDecommission=0
    
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
	
	
# Save the current version of excludes file
        # cp $excludesFile $excludesFile.org
	
# Read and Update excludes file
# Assumption: excludesFile does not have duplicates
	excludesList=( `cat $excludesFile` )
	
	while read ttDecommission; do	    

	    for tt in ${excludesList[@]}; do
		if [ "$tt" = "$ttDecommission" ]; then
		    echo "WARNING: $tt already exists!" 
		    dupl=$((dupl+1))
		    flagDupl=1
		    continue
		fi
	    done 
	    
	    if [ $flagDupl -eq 0 ]; then
		echo "INFO: Adding $ttDecommission to excludes file"
		echo $ttDecommission >> $excludesFile
		returnVal=$?
		if [ $returnVal -ne 0 ]; then
			echo "ERROR: Error while trying to update excludes file"
			exit $ERROR_EXCLUDES_FILE_UPDATE
		fi
		isActive arrActiveTTs[@] $ttDecommission
		returnVal=$?
		if [ $returnVal -eq 1 ]; then
		    numToDecommission=$((numToDecommission+1))
		else
		    echo "WARNING: $ttDecommission is currently not active!"
		    inactiveTT=$((inactiveTT+1))
		fi
	    else
		flagDupl=0
	    fi
	    
	done < $dListFile
	
	echo "INFO: Successfully updated excludes file. Latest excludes file: " 
	cat $excludesFile 
	
# Run decommission script by refreshing hosts
	$hadoopHome/bin/hadoop mradmin -refreshNodes 2> $JTERRFILE
	
	if [ -s $JTERRFILE ]; then
	    parseJTErrFile $JTERRFILE
	    exit $?
	fi
	
        #sleep 2
        #wait

# Determine if all the TTs to be decommissioned are actually decommissioned
	numActiveTTs=${#arrActiveTTs[@]}
	getNumFailDecommission $numActiveTTs $numToDecommission $hadoopHome
	numFailDecommission=$?
	
        # cp $excludesFile $excludesFile.current
        # mv $excludesFile.org $excludesFile
	
# Run refresh hosts again to allow the TTs to join at a later time 
        # $hadoopHome/bin/hadoop mradmin -refreshNodes

    } 200>$LOCKFILE

	lockExitVal=$?
	
    exitWithWarningOrError $numFailDecommission $numToDecommission $dupl $inactiveTT $lockExitVal

    echo "INFO: Successfully decommissioned all $numToDecommission TTs in $dListFile" 
    exit 0	    
}

main $*