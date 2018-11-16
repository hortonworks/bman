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
import uuid

import fabric
from fabric.api import hide, execute
from fabric.contrib.files import exists as remote_exists
from fabric.decorators import task
from fabric.operations import sudo
from fabric.state import env

import bman.constants as constants
from bman.service_installers.hadoop_installer import do_hadoop_install
from bman.service_installers.tez_installer import do_tez_install
from bman.kerberos_setup import do_kerberos_install
from bman.local_tasks import sshkey_gen, sshkey_install, copy_private_key, generate_workers_file, \
    generate_hadoop_env, generate_logging_properties
from bman.logger import get_logger
from bman.remote_tasks import shutdown, run_yarn
from bman.utils import copy_tez_config_files

"""
This module contains support methods for performing cluster deployment
(corresponding to the 'deploy' step in bman).
"""


def make_install_dir(cluster):
    with hide('status', 'warnings', 'running', 'stdout', 'stderr', 'user', 'commands'):
        if not execute(make_base_install_dir, hosts=cluster.get_all_hosts(), cluster=cluster):
            get_logger().error('Making install directory failed.')
            return False


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
    if not os.path.isfile(cluster.get_config(constants.KEY_HADOOP_TARBALL)):
        get_logger().error("Tarball file {} not found.".format(cluster.get_config(constants.KEY_HADOOP_TARBALL)))
        return False

    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        setup_passwordless_ssh(cluster, cluster.get_all_hosts())

    make_install_dir(cluster=cluster)
    make_hadoop_log_dirs(cluster=cluster)
    do_kerberos_install(cluster)
    deploy_common_config_files(cluster)
    do_hadoop_install(cluster=cluster, cluster_id=cluster_id)
    do_tez_install(cluster)

    if stop_services:
        shutdown(cluster)
    else:
        run_yarn(cluster=cluster)

    return True


def make_hadoop_log_dirs(cluster=None):
    """
    Make log output directories for all Hadoop services.
    :param cluster:
    :return:
    """
    targets = cluster.get_all_hosts()
    if not execute(task_make_hadoop_log_dirs, hosts=targets, cluster=cluster):
        get_logger().error('Failed to create log directories')
        return False


@task
def make_base_install_dir(cluster):
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


def setup_passwordless_ssh(cluster, targets):
    # Setup password-less ssh for all service users.
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
def task_make_hadoop_log_dirs(cluster=None):
    logging_root = os.path.join(cluster.get_hadoop_install_dir(), constants.HADOOP_LOG_DIR_NAME)
    get_logger().debug("Creating log output dir {} on host {}".format(logging_root, env.host))
    sudo('mkdir -p {}'.format(logging_root))
    sudo('chgrp {} {}'.format(constants.HADOOP_GROUP, logging_root))
    sudo('chmod 775 {}'.format(logging_root))


def deploy_common_config_files(cluster):
    """
    Deploy config files that are used by multiple services.
    E.g. workers file, hadoop-env.sh.

    These files must be present on every bman deployed cluster.
    :param cluster:
    :return:
    """

    # We create configuration files in the generated directory. Once that
    # is done, we process specific files that need template processing
    # and over write them. In other words the copy of all files need to be
    # first.
    check_for_generated_dirs(cluster)
    copy_all_configs(cluster)

    generate_workers_file(cluster)
    generate_hadoop_env(cluster)
    generate_logging_properties(cluster)


if __name__ == '__main__':
    pass
