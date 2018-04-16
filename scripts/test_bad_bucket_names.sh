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
# This script passes a set of bad names to 
# ozone server and makes sure we get failed
# response from OZONE
#

RED=$(tput setaf 1)
NORMAL=$(tput sgr0)

names=("BUCKET" "bucket-" "bucket.." "10.1.1.1" "xy" "bucket@$" "-bucket" "bu..cket" "bucket." ) 
for i in "${names[@]}"
do 
	source ./create_bucket.sh $i 
	if (( ((HTTP_STATUS < 400)) || (( HTTP_STATUS == 0)) )); then
		printf "${RED}Test Failed. Bucket \"$i\" got created or service not responsive. ${NORMAL}\n"
	fi
done