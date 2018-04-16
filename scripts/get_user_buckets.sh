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
# This script creates a bucket in Ozone
#

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <user>" >&2
  echo "list all buckets owned by this user" 
  exit 1
fi

## check for verbose, helps with debugging
## To Enable verbose logging, do export OZONE_VERBOSE=1
VERBOSE="-s -o /dev/null"
if [ -n "$OZONE_VERBOSE" ]; then 
	VERBOSE="-v "
fi

## Overwrite AUTH for testing purpose. Overwrite with 
## spaces or some junk value if the Authorization KEY
## is missing OZONE should fail this call

AUTH_DEFAULT="Authorization:OZONE $1:ZW5naW5lZXIK"
: ${AUTH:=$AUTH_DEFAULT}

VERB="GET"
URL="http://localhost:8080/"


HTTP_STATUS=$(curl $VERBOSE -X $VERB --write-out '%{http_code}' -H "$AUTH"   $URL)
if [ -n "$OZONE_VERBOSE" ]; then 
	HTTP_STATUS=$(expr $HTTP_STATUS + 0)
fi
echo -e "Command:List Buckets:\t\t$VERB $URL --> return status: $HTTP_STATUS"
