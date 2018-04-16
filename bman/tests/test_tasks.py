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

# This file contains test for tasks by command shell and the script.

import sys

from bman import constants

sys.path.insert(0, '../')
from fabric.api import env, execute
from bman.tasks.remote_tasks import check_users_exist


def test_check_user_exists():
    """
    checks that user hdfs exists on the given machine. This test will prompt for
    a password.
    """
    env.hosts = ['mynode1.example.com', 'mynode2.example.com', 'mynode3.example.com']

    # if you have key based authentication, uncomment and point to private key
    # env.key_filename = '~/.ssh/aengineerkey'
    # if you have password based authentication

    result = execute(check_users_exist, hosts=env.hosts, username=constants.HDFS_USER)
    print(result)
