#!/usr/bin/env bash



if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <volume> <bucket> <port>" >&2
  echo "creates a volume and buckets in in ozone server"
  echo "port is optional, default value is 50075"
  exit 1
fi

FILENAME=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 32)
cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 10240 | gzip -cf > $FILENAME.data.zip

VOLURL="http://localhost:${3:-9864}/$1"
echo "creating volume : $VOLURL"
./hdfs oz -createVolume "$VOLURL"  -user bilbo -root
rc=$?; if [[ $rc != 0 ]]; then  echo "exit : $rc"; exit $rc; fi

echo "Info Volume :"
./hdfs oz -infoVolume "$VOLURL"  -user bilbo -root
rc=$?; if [[ $rc != 0 ]]; then  echo "exit : $rc"; exit $rc; fi


BUCKETURL="http://localhost:${3:-9864}/$1/$2"
echo "Creating bucket : $BUCKETURL"
./hdfs oz -createBucket "$BUCKETURL"  -user bilbo -root
rc=$?; if [[ $rc != 0 ]]; then  echo "exit : $rc"; exit $rc; fi


echo "Info bucket : "
./hdfs oz -infoBucket "$BUCKETURL"  -user bilbo -root
rc=$?; if [[ $rc != 0 ]]; then echo "exit : $rc"; exit $rc; fi


KEYURL="http://localhost:${3:-9864}/$1/$2/$FILENAME.data.zip"
echo "uploading file to key : $KEYURL"
./hdfs oz -putKey "$KEYURL"  -file  $FILENAME.data.zip  -user bilbo -root
rc=$?; if [[ $rc != 0 ]]; then  echo "exit : $rc"; exit $rc; fi

mv $FILENAME.data.zip $FILENAME.data.zip.old
./hdfs oz -getKey "$KEYURL"  -file $FILENAME.data.zip  -user bilbo -root
rc=$?; if [[ $rc != 0 ]]; then  echo "exit : $rc"; exit $rc; fi

hash=`md5 -q $FILENAME.data.zip.old`
echo "uploaded file hash   : $hash"

hash=`md5 -q $FILENAME.data.zip`
echo "downloaded file hash : $hash"

echo "List Keys : "
./hadoop-3.0.0-SNAPSHOT/bin/hdfs oz -listKey $BUCKETURL
