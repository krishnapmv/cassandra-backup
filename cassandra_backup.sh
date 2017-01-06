#!/bin/bash

#
# Description: Script to take snapshot of data and schema of all keyspaces in cassandra.
# This acts as local backups and if we enable, incremental backups in cassandra configuration,
# it will enable us to perform point in time recoveries.
#
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

DATE=`date +%Y%m%d%H%M%S`
SNAME="snapshot-$DATE"
SCHEMA_NAME="schema-$DATE"
BACKUP_SNAPSHOT_DIR="/data/db/backups/snapshots"
BACKUP_SCHEMA_DIR="/data/db/backups/schema"
CASSANDRA_CONFIG="/etc/cassandra/cassandra.yaml"
SCHEMA_FILE="/tmp/keyspace_name_schema.cql"

if [ ! -d "$BACKUP_SNAPSHOT_DIR" ]; then
        echo "Directory $BACKUP_SNAPSHOT_DIR not found, creating..."
        mkdir -p $BACKUP_SNAPSHOT_DIR
fi

if [ ! -d "$BACKUP_SCHEMA_DIR" ]; then
        echo "Directory $BACKUP_SCHEMA_DIR not found, exit..."
	mkdir -p $BACKUP_SCHEMA_DIR
fi

echo "Snapshot name: $SNAME"
#Clear previous snapshots before taking a new one
nodetool -h 127.0.0.1 clearsnapshot

if [[ $? != 0 ]]; then
        echo "Can't clear snapshots"
        exit
fi
 
# Take a snapshot
nodetool -h 127.0.0.1 snapshot -t $SNAME

if [[ $? != 0 ]]; then
        echo "Can't take snapshots"
        exit
fi

#Parse cassandra configs to extract info on data directories. In our env, we've multiple data directories
eval $( parse_yaml "$CASSANDRA_CONFIG" )

for directory in ${data_file_directories[@]}; do
	DATADIR="$directory"
	SFILES=`ls -1 -d $DATADIR/*/*/snapshots/$SNAME`

	#Move snapshot files
	for f in $SFILES
	do
        	#echo "Process snapshot $f"
        	TABLE=`echo $f | awk -F/ '{print $(NF-2)}'`
        	KEYSPACE=`echo $f | awk -F/ '{print $(NF-3)}'`
 
         	if [ ! -d "$BACKUP_SNAPSHOT_DIR/$SNAME/$KEYSPACE/$TABLE" ] ; then
			mkdir -p $BACKUP_SNAPSHOT_DIR/$SNAME/$KEYSPACE/$TABLE
		fi
        	find $f -maxdepth 1 -type f -exec mv -t $BACKUP_SNAPSHOT_DIR/$SNAME/$KEYSPACE/$TABLE/ {} +
	done

	#Clear backup files; Old backup files are not important after snapshot
	BFILES=`ls -1 -d $DATADIR/*/*/backups/`
	for f in $BFILES
	do
        	#echo "Clear $f"
        	rm -f $f*
	done
done


#Schema backup now

## List All Keyspaces
cqlsh -e "DESC KEYSPACES"  > $SCHEMA_FILE

if [ ! -d "$BACKUP_SCHEMA_DIR/$SCHEMA_NAME" ]; then
	mkdir -p $BACKUP_SCHEMA_DIR/$SCHEMA_NAME
fi

## Take SCHEMA Backup - All Keyspace and All tables
for keyspace in $(cat $SCHEMA_FILE); do
	cqlsh -e "DESC KEYSPACE  $keyspace" > "$BACKUP_SCHEMA_DIR/$SCHEMA_NAME/$keyspace.cql"
done
