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


## check for verbose, helps with debugging
## To Enable verbose logging, do export OZONE_VERBOSE=1
VERBOSE="-s -o /dev/null"
if [ -n "$OZONE_VERBOSE" ]; then
	VERBOSE="-i"
fi

## Overwrite AUTH for testing purpose. Overwrite with
## spaces or some junk value if the Authorization KEY
## is missing OZONE should fail this call
## We have to be to root or hdfs user for this call to succeed
AUTH_DEFAULT="Authorization:OZONE hdfs"
: ${AUTH:=$AUTH_DEFAULT}

VERB="GET"
URL="http://localhost:8080/"
OZONE_VERSION="x-ozone-version:v1"
OZONE_USER="x-ozone-user:bilbo"
DATE="Date:$(LC_ALL=C date -u +"%a, %d %b %Y %X %z")"
TYPE="Content-Type: application/json"

HTTP_STATUS=$(curl $VERBOSE -X $VERB --write-out '%{http_code}' -H "$AUTH" -H "$OZONE_VERSION" -H "$OZONE_USER" -H "$TYPE" -H"$DATE" $URL)
echo -e "Command:List volumes:\t\t$VERB $URL --> return status: \n$HTTP_STATUS"
