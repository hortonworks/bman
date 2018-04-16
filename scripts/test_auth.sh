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
# This script verifies the OZONE honors AUTH
# flag properly.
#

RED=$(tput setaf 1)
NORMAL=$(tput sgr0)

FILENAME=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 32)
source ./create_bucket.sh $FILENAME 
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
## Please NOTE : Error 0 is not shell error code, it is the value
## that a CURL call will leave in the HTTP_STATUS code.
##
## HTTP STATUS codes start with 10x and go till 50x.
## I know this can be confusing in a shell script -- My apologies.
## if there is NO HTTP Server end point we will get 0 as the status code

		printf "${RED}Test Failed. Bucket creation failed. ${NORMAL}\n"
fi

## Now try without the AUTH flag, and the call should fail.
FILENAME=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 32)
AUTH="test" source  ./create_bucket.sh $FILENAME
if (( ((HTTP_STATUS < 400)) || (( HTTP_STATUS == 0)) )); then
		printf "${RED}Test Failed. Bucket created. Expected to fail ${NORMAL}\n"
fi


## This test should work
FILENAME=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 32)
source  ./create_bucket.sh $FILENAME
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
		printf "${RED}Test Failed. Bucket creation/deletion failed.${NORMAL}\n"
fi

source ./delete_bucket.sh $FILENAME
if (( ((HTTP_STATUS > 399)) || (( HTTP_STATUS == 0)) )); then
		printf "${RED}Test Failed. Bucket creation/deletion failed.${NORMAL}\n"
fi

## This should FAIL - As auth is over written
FILENAME=$(cat /dev/urandom | env LC_CTYPE=C tr -dc 'a-z0-9' | head -c 32)
AUTH="x-test:test" source ./create_bucket.sh $FILENAME
if (( ((HTTP_STATUS < 400)) || (( HTTP_STATUS == 0)) )); then
		printf "${RED}Test Failed. Bucket created/deleted. Expected to fail ${NORMAL}\n"
fi

