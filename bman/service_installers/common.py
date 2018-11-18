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

import os
import re
import shutil
import uuid
from string import Template

import fabric
from fabric.api import hide, execute, put
from fabric.contrib.files import exists as remote_exists
from fabric.decorators import task, parallel
from fabric.operations import sudo
from fabric.state import env
from pkg_resources import resource_string, resource_listdir

from bman import constants as constants
from bman.service_installers.hadoop_installer import do_hadoop_install
from bman.service_installers.tez_installer import do_tez_install, get_hadoop_env_tez_settings
from bman.kerberos_setup import do_kerberos_install
from bman.local_tasks import sshkey_gen, sshkey_install, copy_private_key
from bman.logger import get_logger
from bman.remote_tasks import shutdown, run_yarn

"""
This module contains support methods for performing cluster deployment
(corresponding to the 'deploy' step in bman).

It delegates to service-specific installers in the same package.

The main entry point is the install_cluster method.
"""


def install_cluster(cluster_id=uuid.uuid4(), cluster=None, stop_services=True):
    """
    Install services from the supplied configuration.

    :param cluster_id: UUID of the new cluster.
    :param cluster: 'Cluster' object that contains the cluster configurations.
    :param stop_services: if True, then services are stopped after installation.
    :return:
    """
    fabric.state.output.status = False
    env.output_prefix = False
    get_logger().info("Deploying to {} nodes. This will take a few minutes.".format(
        len(cluster.get_all_hosts())))

    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        __setup_passwordless_ssh(cluster, cluster.get_all_hosts())

    __make_install_dir(cluster=cluster)
    __make_hadoop_log_dirs(cluster=cluster)
    do_kerberos_install(cluster)
    do_hadoop_install(cluster=cluster, cluster_id=cluster_id)
    __deploy_common_config_files(cluster)
    do_tez_install(cluster)

    if stop_services:
        shutdown(cluster)
    else:
        run_yarn(cluster=cluster)

    return True


def __make_install_dir(cluster):
    """ Make the top-level install directory on all cluster nodes. """
    with hide('status', 'warnings', 'running', 'stdout', 'stderr', 'user', 'commands'):
        if not execute(__make_base_install_dir, hosts=cluster.get_all_hosts(), cluster=cluster):
            get_logger().error('Making install directory failed.')
            return False


def __make_hadoop_log_dirs(cluster):
    """
    Make log output directories for all Hadoop services.
    :param cluster:
    :return:
    """
    targets = cluster.get_all_hosts()
    if not execute(__task_make_hadoop_log_dirs, hosts=targets, cluster=cluster):
        get_logger().error('Failed to create log directories')
        return False


@task
def __make_base_install_dir(cluster):
    """
    Creates a new install directory after backing up the old one.
    All service binaries and config files will be placed under this
    directory.
    """
    install_dir = cluster.get_hadoop_install_dir()
    backup_dir = '{}/{}.backup'.format(cluster.get_config(constants.KEY_INSTALL_DIR),
                                       cluster.get_hadoop_distro_name())

    get_logger().debug("Making install dir {} on host {}".format(install_dir, env.host))
    if remote_exists(install_dir):
        sudo('rm -rf {}'.format(backup_dir))
        sudo('mv {} {}'.format(install_dir, backup_dir))
    sudo('mkdir -p {}'.format(install_dir))
    sudo('chmod 0755 {}'.format(install_dir))
    return True


def __setup_passwordless_ssh(cluster, targets):
    """ Setup password-less ssh for all service users """
    get_logger().info("Installing ssh keys for users [{}] on {} hosts.".format(
        ", ".join(cluster.get_service_user_names()), len(targets)))
    for user in cluster.get_service_users():
        sshkey_gen(cluster=cluster, user=user)
        for hostname in targets:
            sshkey_install(hostname=hostname, user=user, cluster=cluster)
        if not execute(copy_private_key, hosts=targets, user=user, cluster=cluster):
            get_logger().error('Putting private key failed.')
            return False


@task
def __task_make_hadoop_log_dirs(cluster):
    logging_root = os.path.join(cluster.get_hadoop_install_dir(), constants.HADOOP_LOG_DIR_NAME)
    get_logger().debug("Creating log output dir {} on host {}".format(logging_root, env.host))
    sudo('mkdir -p {}'.format(logging_root))
    sudo('chgrp {} {}'.format(constants.HADOOP_GROUP, logging_root))
    sudo('chmod 775 {}'.format(logging_root))


def __deploy_common_config_files(cluster):
    """
    Deploy files that are used by multiple services.
     - hadoop-env.sh
     - workers
     - log4j.properties
     - ssh keys.
    
    Hadoop is somewhat specially.

    These files must be present on every bman deployed cluster.
    :param cluster:
    :return:
    """
    __create_tmp_dirs_for_common_config_files(cluster)
    __generate_workers_file(cluster)
    __generate_hadoop_env(cluster)
    __generate_logging_properties(cluster)
    if not execute(__distribute_common_config_files,
                   hosts=cluster.get_all_hosts(), cluster=cluster,
                   source_dir=cluster.get_generated_hadoop_conf_tmp_dir(),
                   files=['hadoop-env.sh', 'workers', 'log4j.properties']):
        get_logger().error('copying config files failed.')
        return False
    __copy_config_files_to_tmp_dir(cluster)


def __create_tmp_dirs_for_common_config_files(cluster):
    """
    Create local directories to store generated config files
    and ssh keys.
    
    These will eventually be copied to the cluster nodes.
    :param cluster: 
    :return: 
    """
    for d in [cluster.get_generated_hadoop_conf_tmp_dir(),
              cluster.get_ssh_keys_tmp_dir()]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)


def __generate_workers_file(cluster):
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


def __generate_hadoop_env(cluster):
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
        env_str = env_str + get_hadoop_env_tez_settings(cluster)

    with open(os.path.join(cluster.get_generated_hadoop_conf_tmp_dir(), "hadoop-env.sh"), "w") as hadoop_env:
        hadoop_env.write(env_str)


def __generate_logging_properties(cluster):
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


def __copy_config_files_to_tmp_dir(cluster):
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


@task
def __distribute_common_config_files(cluster, source_dir, files):
    """
    Copy workers, hadoop-env.sh and log4j.properties to a given cluster node.

    This is a case where Hadoop is slightly special since hadoop-env.sh
    is used by Ozone also, even if we are not installing HDFS+YARN.

    :param cluster:
    :param source_dir: local directory that has the generated config files.
    :param files: a list of config files to copy to the cluster node.
    """
    for f in files:
        src = os.path.join(source_dir, f)
        get_logger().info("Copying {} to {}:{}".format(src, env.host, cluster.get_hadoop_conf_dir()))
        put(src, os.path.join(cluster.get_hadoop_conf_dir(), f), use_sudo=True)


if __name__ == '__main__':
    pass

