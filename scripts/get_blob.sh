#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
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
# This script gets a file from an existing bucket in ozone.
#

RED=$(tput setaf 1)
NORMAL=$(tput sgr0)

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <file> <bucket>" >&2
  echo "gets a file from an existing bucket" 
  exit 1
fi

## delete the local file before we download the file
if [ -f $1 ]; then
 rm $1
fi

## check for verbose, helps with debugging
## To Enable verbose logging, do export OZONE_VERBOSE=1
VERBOSE="-s "
if [ -n "$OZONE_VERBOSE" ]; then 
	VERBOSE="-v"
fi


## Overwrite AUTH for testing purpose. Overwrite with 
## spaces or some junk value if the Authorization KEY
## is missing OZONE should fail this call
AUTH_DEFAULT="Authorization:OZONE YmlsYm8K:ZW5naW5lZXIK"
: ${AUTH:=$AUTH_DEFAULT}


VERB="GET"
URL="http://localhost:8080/$2/$1"
HOST="Host:bilbohobbit.ozone.self:8080"

HTTP_STATUS=$(curl $VERBOSE -o $1 -X $VERB -w '%{http_code}' -H "$AUTH" -H $HOST $URL)
if [ -n "$OZONE_VERBOSE" ]; then 
	HTTP_STATUS=$(expr $HTTP_STATUS + 0)
fi

if [ ! -f $1 ]; then
 printf "${RED}$1 : unable to download the specified file${NORMAL}\n"
fi

echo -e "Command:Get Object:\t\t$VERB $URL --> return status: $HTTP_STATUS"

