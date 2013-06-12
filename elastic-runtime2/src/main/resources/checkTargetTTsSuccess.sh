#!/bin/bash

# This script checks if the active target TTs were reached
# Prerequisites:
# * This script is deployed on the jobtracker VM
# * The jobtracker is running  
# 
# USAGE: $ME <targetActiveTTs> <ExcludesFile> <HadoopHome> 

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
ERROR_BAD_HADOOP_HOME=103
ERROR_JT_CONNECTION=104
ERROR_JT_UNKNOWN=105
ERROR_FEWER_TTS=107
ERROR_EXCESS_TTS=108
ERROR_BAD_TARGET_TTS=109
ERROR_LOCK_FILE_WRITE=111
WARN_IGNORE=202

# Check script arguments 

checkArguments()
{
    if [ $# -ne $EXPECTED_ARGS ]; then
	    echo "USAGE: $ME <targetActiveTTs> <HadoopHome>"
	    exit $ERROR_BAD_ARGS
    fi
    
    local loc_numTargetTTs=$1
    local loc_excludesFile=$2
    local loc_hadoopHome=$3
    
    if [[ $loc_numTargetTTs -lt 0 ]]; then
	echo "ERROR: Bad number of targetTTs - $loc_numTargetTTs"
	exit $ERROR_BAD_TARGET_TTS
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

# Parse the error file generated for JobTracker

parseJTErrFile()
{
    file="$1"

    firstWord=`head -1 $file | awk '{print $1}'`
    numLines=`wc -l $file | awk '{print $1}'`

# If first line says DEPRECATED use of "old" bin/hadoop and that is
# the only warning, ignore it for now...
    if [[ "$firstWord" = "DEPRECATED:" && $numLines -eq 3 ]]; then
        echo "WARNING: Using a DEPRECATED command (e.g., bin/hadoop instead of bin/mapred)"
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

# check if active targets match the requested number of TTs

checkTargetActiveTTs()
{
    local loc_numTargetTTs=$1
    local loc_hadoopHome=$2

    local loc_mismatchTTs=0    

    local loc_ctr=0

    while true; do
	newActiveTTs=`$loc_hadoopHome/bin/hadoop job -list-active-trackers 2> $JTERRFILE`
	arrNewActiveTTs=( $newActiveTTs )

        if [ -s $JTERRFILE ]; then
            parseJTErrFile $JTERRFILE
            returnVal=$?
            if [ $returnVal -ne $WARN_IGNORE ]; then
                exit $?
            fi
        fi

	numNewActiveTTs=${#arrNewActiveTTs[@]}
	
	if [[ $numNewActiveTTs -ne $loc_numTargetTTs ]]; then
		loc_ctr=$((loc_ctr+1))
		if [[ $loc_ctr -eq 10 ]]; then
		    loc_mismatchTTs=$((numNewActiveTTs - loc_numTargetTTs))
		    break
		else
		    echo "Waiting # $loc_ctr"
		    sleep 1
		fi
	    else
		break
	    fi
    done

    if [[ $loc_mismatchTTs -eq 0 ]]; then
	return 0
    elif [[ $loc_mismatchTTs -lt 0 ]]; then
	return 1
    else 
	return 2
    fi
}

# Print Active TTs (to be sent back to remote caller)
printActiveTTs()
{
    local loc_hadoopHome=$1

    newActiveTTs=`$loc_hadoopHome/bin/hadoop job -list-active-trackers 2> $JTERRFILE`
    arrNewActiveTTs=( $newActiveTTs )
    
    if [ -s $JTERRFILE ]; then
        parseJTErrFile $JTERRFILE
        returnVal=$?
        if [ $returnVal -ne $WARN_IGNORE ]; then
            exit $?
        fi
    fi

# List of TT names (after removing initial tracker_)    
    for tt in ${arrNewActiveTTs[@]}; do	
	echo "$tt" | cut -d: -f1 | cut -d_ -f1 --complement
    done
}

main()
{
# Remove logfile if present
    rm -f $LOGFILE

# Redirect stdout to a log file
    exec 6>&1
    exec > $LOGFILE

# Arguments check/set
    checkArguments $*

    numTargetTTs=$1
    excludesFile=$2
    hadoopHome=$3
        
    echo "INFO: Arguments:: Target for active TTs: $targetActiveTTs; hadoopHome: $hadoopHome" 

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
	
# Determine if target TTs is reached
	checkTargetActiveTTs $numTargetTTs $hadoopHome
	retVal=$?

# Clear excludes file
        > $excludesFile

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


    if [[ $lockExitVal -ne 0 ]]; then
	echo "ERROR: Failed to write to lock file $LOCKFILE (permissions problem?)"
	exitVal=$ERROR_LOCK_FILE_WRITE    
    elif [[ "$retVal" -eq "0" ]]; then
	echo "Successfully reached target number of active TTs: $numTargetTTs"
	exitVal=0
    elif [[ "$retVal" -eq "1" ]]; then 
	echo "Number of active TTs is less than the target: $numTargetTTs"
	exitVal=$ERROR_FEWER_TTS
    else
	echo "Number of active TTs is greater than the target: $numTargetTTs"	
	exitVal=$ERROR_EXCESS_TTS
    fi

# Restore stdout
    exec 1>&6 6>&-

# Print list of ActiveTTs on stdout
    printActiveTTs $hadoopHome


    exit $exitVal
}

main $*
