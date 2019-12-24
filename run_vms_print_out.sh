#!/bin/sh

start_port=7001
COUNTER=0
NUM_VMS=$1
while [  $COUNTER -lt $NUM_VMS ]; do

    echo "Trying port: $start_port"

    if [[ -n $(sudo netstat -lntu | grep ":$start_port ") ]];
    # if sudo netstat -lntu | grep ':$start_port '
    then
        echo "[Error] Port in use"
        let "start_port += 1"
    else
        echo "Running on port:$start_port"
        let COUNTER=COUNTER+1

        RESULT=$(echo 00$COUNTER | tail -c 4)
        go run server.go $start_port > "node$RESULT.log" &
        let "start_port += 1"
    fi
    echo "$COUNTER ports up"

done    
