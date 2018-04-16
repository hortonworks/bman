#!/bin/bash

# Copyright 2016-2018 Hortonworks Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# This script puts creates a bunch of buckets,
# put the file inside the bucket, reads it back
# delete the file and deletes the bucket.
#

function finish {
  if [ -e $FILENAME.data.zip ]; then
    rm $FILENAME.data.zip
  fi
}

trap finish EXIT

RED=$(tput setaf 1)
NORMAL=$(tput sgr0)

BUCKET_NAME=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 32)
FILENAME=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 32)
cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 10240 | gzip -cf > $FILENAME.data.zip

source ./create_bucket.sh $BUCKET_NAME
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
## Please NOTE : Error 0 is not shell error code, it is the value
## that a CURL call will leave in the HTTP_STATUS code.
##
##  HTTP STATUS codes start with 10x and go till 50x.
## I know this can be confusing in a shell script -- My apologies.
        printf "${RED}Test Failed. Bucket \"$BUCKET_NAME\" not created. ${NORMAL}\n"
        exit 1
fi


source ./put_blob.sh $FILENAME.data.zip $BUCKET_NAME
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
        printf "${RED}Test Failed. $FILENAME.data.zip Upload failed. ${NORMAL}\n"
        exit 1
fi

source ./get_blob.sh $FILENAME.data.zip $BUCKET_NAME
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
        printf "${RED}Test Failed. $FILENAME.data.zip download failed. ${NORMAL}\n"
        exit 1
fi

source ./delete_blob.sh $FILENAME.data.zip $BUCKET_NAME
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
        printf "${RED}Test Failed. $FILENAME.data.zip delete failed. ${NORMAL}\n"
        exit 1
fi

source ./delete_bucket.sh $BUCKET_NAME
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
        printf "${RED}Test Failed. $deleting $BUCKET_NAME failed. ${NORMAL}\n"
        exit 1
fi




