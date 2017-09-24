#!/bin/bash

#
#
# Script to take snapshots/backups and ship them to AWS S3
#


export PATH=/sbin:/bin:/usr/sbin:/usr/bin
 
#Function to parse yaml file
function parse_yaml() {
        # Basic (as in imperfect) parsing of a given YAML file.  Parameters
        # are stored as environment variables.
        local prefix=$2
        local s
        local w
        local fs
        s='[[:space:]]*'
        w='[a-zA-Z0-9_]*'
        fs="$(echo @|tr @ '\034')"
        sed -ne "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|\1$fs\2$fs\3|p" \
            -e "s|^\($s\)\($w\)$s[:-]$s\(.*\)$s\$|\1$fs\2$fs\3|p" "$1" |
        awk -F"$fs" '{
          indent = length($1)/2;
          if (length($2) == 0) { conj[indent]="+";} else {conj[indent]="";}
          vname[indent] = $2;
          for (i in vname) {if (i > indent) {delete vname[i]}}
          if (length($3) > 0) {
            vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
            printf("%s%s%s%s=(\"%s\")\n", "'"$prefix"'",vn, $2, conj[indent-1],$3);
          }
        }' | sed 's/_=/+=/g'
}

#Function to take a snapshot
function take_snapshot() {
	#Clear previous snapshots before taking a new one
	$NODETOOL -h 127.0.0.1 clearsnapshot

	if [[ $? != 0 ]]; then
        	echo "Can't clear snapshots"
        	exit
	fi

	# Take a snapshot
	$NODETOOL -h 127.0.0.1 snapshot -t $SNAME

	if [[ $? != 0 ]]; then
        	echo "Can't take snapshots"
        	exit
	fi

	#Parse cassandra configs to extract info on data directories. In our env, we've multiple data directories
	eval $( parse_yaml "$CASSANDRA_CONFIG" )

	for directory in ${data_file_directories[@]}; do
		DATADIR="$directory"
		SFILES=`ls -1 -d $DATADIR/*/*/snapshots/$SNAME`
	
		for f in $SFILES
		do
			file="$cluster_name/week_$WEEKOFYEAR/$HOSTNAME$f"
			$AWSCLI s3 sync $f s3://${S3_BUCKET}/$file
		done

		#Clear incremental backup files; These are not important after snapshot
		BFILES=`ls -1 -d $DATADIR/*/*/backups/`
		for f in $BFILES
		do
        		#echo "Clear $f"
        		rm -f $f*
		done
	done

	# Backup Schema
	CASIP=$(hostname -I)
	$CQLSH $CASIP -e "DESC KEYSPACES"  > /tmp/schemas.txt
	
	for keyspace in $(cat /tmp/schemas.txt); do
        	$CQLSH $CASIP -e "DESC KEYSPACE  $keyspace" > "/tmp/$keyspace.cql"
		$AWSCLI s3 cp /tmp/$keyspace.cql s3://$S3_BUCKET/$cluster_name/week_$WEEKOFYEAR/$keyspace.cql
	done


	#Backup tokens
	$NODETOOL ring | grep $CASIP | awk '{print $NF ", "}' | xargs > /tmp/tokens.txt
	$AWSCLI s3 cp /tmp/tokens.txt s3://$S3_BUCKET/$cluster_name/week_$WEEKOFYEAR/$HOSTNAME/tokens.txt
}

function copy_backups() {

	#Run nodetool flush to flush memtables to disk before taking backup

	$NODETOOL flush

	#Parse cassandra configs to extract info on data directories. In our env, we've multiple data directories
        eval $( parse_yaml "$CASSANDRA_CONFIG" )
	for directory in ${data_file_directories[@]}; do
		BFILES=`ls -1 -d $directory/*/*/backups/`
		for f in $BFILES
                do
			file="$cluster_name/week_$WEEKOFYEAR/$HOSTNAME$f"
			#echo "uploading $f"
			$AWSCLI s3 sync $f s3://$S3_BUCKET/$file --delete
                done
	done
}

CASSANDRA_CONFIG="/etc/cassandra/cassandra.yaml"
DATE=$(date +%Y%m%d%H%M%S)
DAY=$(date ++%Y%m%d)
DAYOFWEEK=$(date +%u)
WEEKOFYEAR=$(date +%V)
HOSTNAME=$(hostname)

SNAME="snapshot-$DATE"
NODETOOL=$(which nodetool)
CQLSH=$(which cqlsh)
AWSCLI="/usr/local/bin/aws"
S3_BUCKET="tc-cassandra"

if [ "${DAYOFWEEK}" -eq 1 ]; then
	take_snapshot
else 
	copy_backups
fi
