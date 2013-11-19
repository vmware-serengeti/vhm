###########################################################################
# Copyright (c) 2013 VMware, Inc. All Rights Reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###########################################################################

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
JTENV="/etc/default/hadoop-0.20-mapreduce"

# Errors/Warnings
ERROR_BAD_ARGS=100
ERROR_EXCLUDES_FILE_NOT_FOUND=101
ERROR_DRLIST_FILE_NOT_FOUND=102
ERROR_BAD_HADOOP_HOME=103
ERROR_JT_CONNECTION=104
ERROR_JT_UNKNOWN=105
ERROR_EXCLUDES_FILE_UPDATE=110
ERROR_LOCK_FILE_WRITE=111
WARN_TT_EXCLUDESFILE=200
WARN_TT_ACTIVE=201
WARN_IGNORE=202

# Determine if the TT is active (running ok)

isActive()
{
    ttList=("${!1}")
    tt=tracker_"$2"

    for ttData in ${ttList[@]}; do
        activeTT=`echo $ttData | cut -d: -f1`
# Depending on network configuration, we may get a trailing period for TT dnsname
# Currently checking in active TT list both with and without trailing period
# TODO: remove trailing period check once hadoop versions (e.g., Intel/GPHD)/Serengeti fix this
        if [[ "$activeTT" = "$tt" || "$activeTT" = "$tt." ]]; then
                return 1
        fi
    done

    return 0
}

# Parse the error file generated for JobTracker
# Handling known issues/non-issues in different distributions
# TODO: A little hacky for now; needs a long term fix

parseJTErrFile()
{
    file="$1"

    firstWord=`head -1 $file | awk '{print $1}'`
    numLines=`wc -l $file | awk '{print $1}'`

# If first line says DEPRECATED use of "old" bin/hadoop and that is
# the only warning, ignore it for now...(seen in Cloudera's distro)
    if [[ "$firstWord" = "DEPRECATED:" && $numLines -eq 3 ]]; then
        echo "WARNING: Using a DEPRECATED command (e.g., bin/hadoop instead of bin/mapred)"
        return $WARN_IGNORE
    fi
    
    thirdWord=`head -1 $file | awk '{print $3}'`

# If first line says INFO and this is the only line, its just some harmless logging
# (seen in MapR 2.1.3)
    
    if [[ "$thirdWord" = "INFO" && $numLines -eq 1 ]]; then
        echo "Just some harmless logging in JTERR file"
        return $WARN_IGNORE
    fi

# If connection error is detected report it differently from an unknown error
    
    connLine=`sed -n '11p' < $file`
#   lastLine=`tail -1 $file` # We could use this only for "hadoop mradmin"
    echo "$connLine"
    arr=( $connLine )
    lidx=${#arr[@]} 
    if [[ $lidx -gt 0 && "${arr[$((lidx-1))]}" = "refused" && "${arr[$((lidx-1))]}" = "Connection" ]]; then
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

exitWithWarningOrError()
{
    local loc_numToRecommission=$1
    local loc_missingTT=$2
    local loc_activeTT=$3
    local loc_lockExitVal=$4
    
    if [[ $loc_lockExitVal -ne 0 ]]; then
	echo "ERROR: Failed to write to lock file $LOCKFILE (permissions problem?)"
	exit $ERROR_LOCK_FILE_WRITE
    fi

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

# Set different environment, if specified
    if [ -f $JTENV ]; then
        . $JTENV # source this environment
    fi
    
# Ensure only one VHM executes this script at any given time
    
# Makes sure we exit if flock fails.
# set -e
    
    { 
# Wait for lock on $LOCKFILE (fd 200) for 10 seconds
# TODO: Test flock failure to exit (e.g., flock ... || exit $ERROR_FLOCK_FAILED)
	flock -x -w 10 200

# Generate list of active task trackers
	activeTTs=`$hadoopHome/bin/hadoop job -list-active-trackers 2> $JTERRFILE`
	
        if [ -s $JTERRFILE ]; then
            parseJTErrFile $JTERRFILE
            returnVal=$?
            if [ $returnVal -ne $WARN_IGNORE ]; then
                exit $?
            fi
        fi

	arrActiveTTs=( $activeTTs )
	
# Commenting this out for now (22Jul13). We decided to clear the excludes file each time
# to stop unneccessary complications related to dhcp/dns, etc.
# Read and Update excludes file
# Assumption: excludesFile does not have duplicates
#	excludesList=( `cat $excludesFile` )
#	
#	while read ttRecommission; do	    
#
#	    for tt in ${excludesList[@]}; do
#		if [ "$tt" = "$ttRecommission" ]; then
#		    flagPresent=1
#		    break
#		fi
#	    done 
#	    
#	    if [ $flagPresent -eq 1 ]; then
#		flagPresent=0
#
#		echo "INFO: Removing $ttRecommission from excludes file"
#		sed -i "/$ttRecommission/d" $excludesFile
#		returnVal=$?
#		if [ $returnVal -ne 0 ]; then
#		    echo "ERROR: Error while trying to update excludes file"
#		    exit $ERROR_EXCLUDES_FILE_UPDATE
#		fi
#		isActive arrActiveTTs[@] $ttRecommission
#		returnVal=$?
#		if [ $returnVal -eq 0 ]; then
#		    numToRecommission=$((numToRecommission+1))
#		else
#		    echo "WARNING: $ttRecommission is currently active!"
#		    numActiveTT=$((numActiveTT+1))
#		fi
#	    else
#		echo "WARNING: $tt is missing in the excludes file: $excludesFile" 
#		missingTT=$((missingTT+1))
#	    fi
#	    
#	done < $rListFile
#	
#	echo "INFO: Successfully updated excludes file. Latest excludes file: " 
#	cat $excludesFile 

        while read ttRecommission; do
               isActive arrActiveTTs[@] $ttRecommission
               returnVal=$?
               if [ $returnVal -eq 0 ]; then
                   numToRecommission=$((numToRecommission+1))
               else
                   echo "WARNING: $ttRecommission is currently active!"
                   numActiveTT=$((numActiveTT+1))
               fi
	done < $rListFile
		
# Clear excludes file
	echo "INFO: Clearing excludes file..." 
        > $excludesFile

# Run recommission script by refreshing hosts
	$hadoopHome/bin/hadoop mradmin -refreshNodes 2> $JTERRFILE
	
        if [ -s $JTERRFILE ]; then
            parseJTErrFile $JTERRFILE
            returnVal=$?
            if [ $returnVal -ne $WARN_IGNORE ]; then
                exit $?
            fi
        fi

    } 200>$LOCKFILE

    lockExitVal=$?

    exitWithWarningOrError $numToRecommission $missingTT $numActiveTT $lockExitVal
    
    echo "INFO: Successfully recommissioned all $numToRecommission TTs in $rListFile" 
    exit 0	    
}

main $*
