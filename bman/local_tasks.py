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

# This file contains local tasks used command shell and the script.

import os
from os.path import expanduser

from fabric.api import task, local, hide, settings
from fabric.operations import put, sudo
from fabric.state import env

import bman.constants as constants
from bman.logger import get_logger


@task
def sshkey_gen(key_name=None):
    """Generates a an ssh key:e.g fab key_gen"""
    results = local('ssh-keygen -t rsa -f {}', key_name)
    return results.succeeded


@task
def remove_fabric_config():
    """
    Removes Fabric config from the users home directory (~/.fabricrc).

    Cached credentials in the .fabricrc file can conflict with
    the credentials used by bman.
    """
    home = expanduser("~")
    fabricconf = ".fabricrc"
    if os.path.isfile(os.path.join(home, fabricconf)):
        os.remove(os.path.join(home, fabricconf))
    return fabricconf, home


def get_config_file_header():
    return """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
<!-- Put site-specific property overrides in this file. -->
<configuration>
"""


def get_config_file_footer():
    return """
</configuration>"""


def generate_site_config(cluster, filename=None, settings_key=None, output_dir=None):
    """ Create an XML config file."""
    with open(os.path.join(output_dir, filename), "w") as site:
        site.write(get_config_file_header())
        site.write(generate_custom_settings(
            cluster.get_config(settings_key)))
        site.write(get_config_file_footer())


def generate_custom_settings(custom_values):
    generated_val = ''
    for k in custom_values:
        generated_val += \
            """
  <property>
    <name>{}</name>
    <value>{}</value>
  </property>
""".format(k, custom_values[k])
    return generated_val


def get_keyname_for_user(user=None):
    return '{}_{}'.format(constants.DEFAULT_SSH_KEY_NAME, user.name)


@task
def sshkey_gen(cluster=None, user=None):
    """Generates a an ssh"""
    keyname = get_keyname_for_user(user=user)
    ssh_keys_dir = cluster.get_ssh_keys_tmp_dir()
    get_logger().debug("Generating a private key for user {}.".format(user.name))
    os.makedirs(ssh_keys_dir, exist_ok=True)
    local("rm -f {}/{}*".format(ssh_keys_dir, keyname))
    local('ssh-keygen -b 2048 -t rsa -f {}/{}  -q -N ""'.format(ssh_keys_dir, keyname))


def check_ssh_copyid():
    with hide('warnings'), settings(warn_only=True):
        result = local('ssh-copy-id', capture=True)
    return result.succeeded


@task
def sshkey_install(cluster=None, hostname=None, user=None):
    if check_ssh_copyid():
        get_logger().error("Please install ssh-copy-id using 'brew install ssh-copy-id' ")
        get_logger().error("Cannot install ssh keys without that.")
        return

    get_logger().debug("Installing ssh key user {} on host {}".format(user.name, hostname))
    with hide('running'):
        local('sshpass -p {} ssh-copy-id -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {}/{}.pub {}@{}'.format(
                user.name, cluster.get_ssh_keys_tmp_dir(),
                get_keyname_for_user(user=user), user.name, hostname))
        get_logger().debug("Done installing ssh key for user {} on host {}".format(user.name, hostname))
    return True


@task
def copy_private_key(cluster=None, user=None):
    key_name = get_keyname_for_user(user=user)
    default_key_name = constants.DEFAULT_SSH_KEY_NAME
    ssh_key_dir = cluster.get_ssh_keys_tmp_dir()

    # Ugly trick to expand the users home directory on the target host.
    user_home_dir = sudo('echo ~{}'.format(user.name)).splitlines()[0]
    put('{}/{}'.format(ssh_key_dir, key_name), '{}/.ssh/{}'.format(user_home_dir, default_key_name))
    put('{}/{}.pub'.format(ssh_key_dir, key_name), '{}/.ssh/{}.pub'.format(user_home_dir, default_key_name))
    sudo('chown {0} {1}/.ssh/* && chmod 600 {1}/.ssh/{2} {1}/.ssh/{2}.pub'.format(
        user.name, user_home_dir, default_key_name))
    get_logger().debug("Done copying private key {} to host {}".format(
        key_name, env.host))
    return True


if __name__ == '__main__':
    pass
