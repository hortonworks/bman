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

import glob
import os

import time

from fabric.context_managers import hide

from bman.kerberos_config_manager import KEYTABS_DEFAULT_DIR
from fabric.tasks import execute

from bman import constants
from fabric.api import task, sudo, env, put, local
from fabric.decorators import parallel
from fabric.operations import run

from bman.logger import get_logger
from fabric.contrib.files import exists as remote_exists


"""
Utilities used by other modules in bman.
"""


@task
def copy(source_file=None, remote_file=None):
    """Copies a file to remote machine if needed."""
    if is_wildcard_path(source_file) or (source_file, remote_file):
        put(source_file, remote_file)
    else:
        get_logger().info('%s with the same hash already exists in destination. '
                          'skipping copy.', source_file)
    return True


@task
def do_untar(tarball=None, target_folder=None, strip_level=0):
    """ untar the tarball to the right location."""
    sudo('mkdir -p {}'.format(target_folder))
    return sudo('tar -poxvzf {} -C {} --strip {}'.format(tarball, target_folder, strip_level)).succeeded


@task
def start_stop_service(cluster, action, service_name, user=None):
    """ Starts or stops a service """
    install_dir = cluster.get_hadoop_install_dir()
    cmd = 'nohup {}/bin/hdfs --daemon {} {}'.format(install_dir, action, service_name)
    get_logger().info('{} {} on {}'.format(action, service_name, env.host_string))
    return sudo(cmd, user=user).succeeded


def get_md5(source_file, local_file):
    """Returns MD5 of a file based on it is local or remote."""
    cmd = get_command(source_file, local_file)
    output = local(cmd, capture=True) if local_file else run(cmd)
    return get_hash_string(output, local_file)


def get_command(source_file, local_file):
    """ Gets the command to run based on OS. Linux vs. OS X"""
    if local_file:
        name = local('uname -s', capture=True)
        if name.startswith('Darwin'):
            return 'md5 -q ' + source_file
        else:
            return 'md5sum ' + source_file
    else:
        # TODO: our remote machines are centos
        return 'md5sum ' + source_file


def get_hash_string(hash_string, local_file):
    """Parses the hash string based on which on we are running on."""
    if local_file:
        name = local('uname -s', capture=True)
        if name.startswith('Darwin'):
            return hash_string.strip()
        else:
            return hash_string.split()[0]
    else:
        return hash_string.split()[0].strip()


def prompt_for_yes_no(msg):
    """returns a bool based on whether the user presses y/n."""
    choice = None
    while not choice or choice.lower() not in ['y', 'n', 'yes', 'no']:
        choice = input('%s (y/n) ' % msg).lower()
    return choice.lower() in ['y', 'yes']


def get_tarball_destination(local_file):
    """
    Get the path for copying a tarball to a remote host.
    """
    return os.path.join('/', 'tmp', os.path.basename(local_file))


@task
def should_copy(source_file, remote_file):
    """Decides if we should copy a file or not by checking hash of the file"""
    if remote_exists(remote_file):
        localmd5 = get_md5(source_file, True)
        remotemd5 = get_md5(remote_file, False)
        return localmd5 == remotemd5
    else:
        return True


@task
@parallel
def run_cmd(cmd_string=None, user=None):
    """
    Run the given command on a remote node.
    If user is not supplied then run as root.
    """
    if user:
        return sudo(cmd_string, user=user).succeeded
    else:
        return sudo(cmd_string).succeeded


@task
def fast_copy(cluster, remote_file=None):
    """
    scp a file from one cluster node to the rest.

    This is useful when deploying from a geographically distant location
    since the uploads can take a while. With this we copy to a namenode
    and then use scp to copy from namenode to datanodes to get fast copy.

    We always run scp as the 'hdfs' user as password-less ssh between
    cluster hosts is guaranteed to work (we set it up during the deploy
    phase).

    The caller must later change permissions on the file on all hosts.
    """
    targets = set(cluster.get_all_hosts()).symmetric_difference({env.host})
    for i, host_name in enumerate(targets):
        scp_cmd = 'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {} {}:{}'.format(
            remote_file, host_name, remote_file)
        get_logger().debug('Copying {} from {} to {} (node {} of {})'.format(
            remote_file, env.host, host_name, i+1, len(targets)))
        get_logger().debug('The copy command is {}'.format(scp_cmd))
        sudo(scp_cmd)
        # saved_password = env.sudo_password
        # env.sudo_password = cluster.get_user_password(constants.HDFS_USER)
        # sudo(scp_cmd, user=constants.HDFS_USER)
        # env.sudo_password = saved_password   # Restore the global fabric environment.


def put_to_all_nodes(cluster=None, source_file=None, remote_file=None):
    """
    Copy a file to all cluster nodes.
    """
    source_node = sorted(list(cluster.get_all_hosts()))[0]
    get_logger().info("Copying the tarball {} to {}.".format(
        source_file, source_node))
    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        if not execute(copy, hosts=source_node, source_file=source_file,
                       remote_file=remote_file):
            get_logger().error('copy failed.')
            return False

    if not execute(fast_copy, hosts=source_node, cluster=cluster, remote_file=remote_file):
        get_logger().error('fast copy failed.')
        return False


def run_dfs_command(cluster=None, cmd=None):
    if cluster.is_kerberized():
        # Prepend a command to login as the hdfs superuser, and append a command
        # to destroy the credentials when done.
        hdfs_headless_principal = '{}@{}'.format(constants.HDFS_USER, cluster.get_realm())
        hdfs_headless_keytab = os.path.join(
            KEYTABS_DEFAULT_DIR, '{}.headless.keytab'.format(constants.HDFS_USER))
        cmd = 'kinit -kt {} {}'.format(hdfs_headless_keytab, hdfs_headless_principal) + \
              ' && ' + cmd + ' && ' + 'kdestroy'

    # Run the command on a NameNode host and as the 'hdfs' user.
    get_logger().debug("Running command '{}'".format(cmd))
    execute(run_cmd, hosts=cluster.get_hdfs_master_config().get_nn_hosts()[0:1],
            cmd_string=cmd, user=constants.HDFS_USER)


def extract_tarball(targets=None, remote_file=None,
                    target_folder=None, strip_level=None):
    get_logger().info("Extracting {} on all nodes".format(remote_file))
    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        if not execute(do_untar, hosts=targets, tarball=remote_file,
                       target_folder=target_folder, strip_level=strip_level):
            get_logger().error("Failed to untar {}".format(remote_file))
            return False


def is_true(input_string):
    """
    Return True if the input is a boolean True, or a string that
    matches 'true' or 'yes' (case-insensitive).

    Return False for all other string inputs.

    Raise ValueError otherwise.
    """
    if isinstance(input_string, bool):
        return input_string # Return as-is
    if isinstance(input_string, str):
        return input_string.lower() in ['true', 'yes']
    raise TypeError("Expected True/False. Got {}".format(input_string))


def is_wildcard_path(source_file):
    return '*' in source_file or '?' in source_file


def do_sleep(seconds):
    get_logger().info("sleeping for {} seconds".format(seconds))
    time.sleep(seconds)


if __name__ == '__main__':
    pass
