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
import re
import shutil
from os.path import expanduser
from string import Template

from fabric.api import task, local, hide, settings
from fabric.operations import put, sudo
from fabric.state import env
from pkg_resources import resource_string, resource_listdir

import bman.constants as constants
from bman.bman_config import load_config
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


def check_for_generated_dirs(cluster=None):
    for d in [cluster.get_generated_hadoop_conf_tmp_dir(),
              cluster.get_generated_tez_conf_tmp_dir(),
              cluster.get_ssh_keys_tmp_dir()]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)


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


def hadoop_env_tez_settings(cluster):
    return """

    # Apache Tez configuration.
    #
    export TEZ_CONF_DIR={0}
    export TEZ_JARS={1}
    export HADOOP_CLASSPATH=${{HADOOP_CLASSPATH}}:${{TEZ_CONF_DIR}}:${{TEZ_JARS}}/*:${{TEZ_JARS}}/lib/*        
    """.format(cluster.get_tez_conf_dir(), cluster.get_tez_install_dir())


def generate_hadoop_env(cluster):
    """ Generate hadoop-env.sh."""
    get_logger().debug("Generating hadoop-env.sh from template")
    template_str = resource_string('bman.resources.conf', 'hadoop-env.sh.template').decode('utf-8')
    env_str = Template(template_str)

    log_dirs = {}
    # Set the log directories for Hadoop service users.
    for user in cluster.get_service_users():
        log_dirs['{}_log_dir_config'.format(user.name)] = os.path.join(
            cluster.get_hadoop_install_dir(), "logs", user.name)

    env_str = env_str.safe_substitute(
        hadoop_home_config=cluster.get_hadoop_install_dir(),
        java_home=cluster.get_config(constants.KEY_JAVA_HOME),
        hdfs_datanode_secure_user=(constants.HDFS_USER if cluster.is_kerberized() else ''),
        hdfs_datanode_user=('root' if cluster.is_kerberized() else constants.HDFS_USER),
        hdfs_user=constants.HDFS_USER,
        yarn_user=constants.YARN_USER,
        jsvc_home=constants.JSVC_HOME,
        **log_dirs)

    if cluster.is_tez_enabled():
        env_str = env_str + hadoop_env_tez_settings(cluster)

    with open(os.path.join(cluster.get_generated_hadoop_conf_tmp_dir(), "hadoop-env.sh"), "w") as hadoop_env:
        hadoop_env.write(env_str)


def generate_logging_properties(cluster):
    """
    Genrate right logging template based on options that are enabled.

    That is if ozone is enabled, then ozone logging is added if cBlock trace is
    enabled then cBlock trace setting is added.
    :param cluster:
    :return:
    """
    logging_templates = ["log4j.properties.template"]

    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        logging_templates.append(os.path.join("ozone.logging.template"))
        if cluster.get_config(constants.KEY_CBLOCK_TRACE):
            logging_templates.append(os.path.join("cblock.tracing.template"))

    with open(os.path.join(cluster.get_generated_hadoop_conf_tmp_dir(), "log4j.properties"), "w") as logging_prop:
        for log_template in logging_templates:
            template_str = resource_string('bman.resources.conf', log_template).decode('utf-8')
            logging_prop.write(template_str)


def copy_all_configs(cluster=None):
    """ Copy the remaining files as-is, removing the .template suffix """
    conf_generated_dir = cluster.get_generated_hadoop_conf_tmp_dir()
    get_logger().debug("Listing conf resources")
    for f in resource_listdir('bman.resources.conf', ''):
        if f.endswith('.template'):
            get_logger().debug("Got resource {}".format(f))
            resource_contents = resource_string('bman.resources.conf', f).decode('utf-8')
            filename = re.sub(".template$", "", f)
            with open(os.path.join(conf_generated_dir, filename), "w") as output_file:
                output_file.write(resource_contents)


def generate_workers_file(cluster):
    """Generates the workers file based on the machines in datanodes list."""
    workers = cluster.get_config(constants.KEY_WORKERS)
    conf_generated_dir = cluster.get_generated_hadoop_conf_tmp_dir()
    with open(os.path.join(conf_generated_dir, 'workers'), 'w') as workers_file:
        for host_name in workers:
            workers_file.write(host_name)
            workers_file.write('\n')

    # Also make a copy named 'slaves' for Hadoop versions 2.x.
    # TODO: Deprecate this eventually.
    shutil.copy2(os.path.join(conf_generated_dir, 'workers'),
                 os.path.join(conf_generated_dir, 'slaves'))


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
