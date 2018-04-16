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
from fabric.api import hide, execute, show
from fabric.contrib.files import exists as remote_exists
from fabric.decorators import task
from fabric.operations import sudo, put
from fabric.state import env

import bman.constants as constants
from bman.kerberos_setup import do_kerberos_install
from bman.local_tasks import generate_configs, sshkey_gen, sshkey_install, copy_private_key
from bman.logger import get_logger
from bman.remote_tasks import do_active_transitions, stop_dfs, stop_yarn, shutdown, start_yarn, run_yarn
from bman.utils import get_tarball_destination, start_stop_service, do_untar, \
    run_dfs_command, do_sleep, put_to_all_nodes, copy_hadoop_config_files, copy_tez_config_files

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
    deploy_hadoop_tarball(cluster=cluster)
    deploy_tez_tarball(cluster=cluster)

    targets = cluster.get_all_hosts()
    if not execute(make_hadoop_log_dirs, hosts=targets, cluster=cluster):
        get_logger().error('Failed to create log directories')
        return False

    # Make the NameNode and DataNode directories.
    get_logger().info("Creating HDFS metadata and data directories")
    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        if not execute(make_hdfs_dirs, hosts=targets, cluster=cluster):
            get_logger().error("Failed to make HDFS directories")
            return False

    generate_configs(cluster)
    get_logger().info('copying config files to remote machines.')
    if not execute(copy_hadoop_config_files, hosts=targets, cluster=cluster):
        get_logger().error('copying config files failed.')
        return False

    if cluster.is_tez_enabled() and not execute(copy_tez_config_files, hosts=targets, cluster=cluster):
        get_logger().error('copying config files failed.')
        return False

    if not execute(copy_jscsi_helper, hosts=targets, cluster=cluster):
        get_logger().error('copying jscsi helper failed.')
        return False

    if not execute(create_ozone_metadata_paths, hosts=targets, cluster=cluster):
        get_logger().error('Create directories failed.')
        return False

    if cluster.is_kerberized():
        get_logger().info("Enabling Kerberos support.")
        do_kerberos_install(cluster)
    else:
        get_logger().info("Cluster is not kerberized.")

    format_hdfs_nameservices(cluster, cluster_id)

    if not execute(start_stop_service, hosts=cluster.get_worker_nodes(), cluster=cluster,
                   action='start', service_name='datanode'):
        get_logger().error("Failed to start one or more DataNodes.")
        return False

    if cluster.is_tez_enabled():
        deploy_tez(cluster)

    get_logger().info("Done deploying Hadoop to {} nodes.".format(len(targets)))

    if stop_services:
        shutdown(cluster)
    else:
        run_yarn(cluster=cluster)

    return True


def start_stop_all_journalnodes(cluster, action=None):
    hdfs_master_config = cluster.get_hdfs_master_config()
    targets = hdfs_master_config.get_jn_hosts()
    if targets:
        return execute(start_stop_service, hosts=targets, cluster=cluster,
                       action=action, service_name='journalnode',
                       user=constants.HDFS_USER)


@task
def make_base_install_dir(cluster):
    """
    Creates a new install directory after backing up the old one.
    All service binaries and config files will be placed under this
    directory.
    """
    install_dir = cluster.get_hadoop_install_dir()
    backup_dir = '{}/{}.backup'.format(cluster.get_config(constants.KEY_HOMEDIR),
                                       cluster.get_hadoop_distro_name())

    get_logger().debug("Making install dir {} on host {}".format(install_dir, env.host))
    if remote_exists(install_dir):
        sudo('rm -rf {}'.format(backup_dir))
        sudo('mv {} {}'.format(install_dir, backup_dir))
    sudo('mkdir -p {}'.format(install_dir))
    sudo('chmod 0755 {}'.format(install_dir))
    return True


def deploy_hadoop_tarball(cluster=None):
    source_file = cluster.get_config(constants.KEY_HADOOP_TARBALL)
    remote_file = get_tarball_destination(source_file)
    put_to_all_nodes(cluster=cluster, source_file=source_file, remote_file=remote_file)
    # The Hadoop tarball has an extra top-level directory, strip it out.
    extract_tarball(cluster=cluster, targets=cluster.get_all_hosts(),
                    remote_file=remote_file,
                    target_folder=cluster.get_hadoop_install_dir(),
                    strip_level=1)


def deploy_tez_tarball(cluster=None):
    if cluster.is_tez_enabled():
        source_file = cluster.get_config(constants.KEY_TEZ_TARBALL)
        remote_file = get_tarball_destination(source_file)
        put_to_all_nodes(cluster=cluster, source_file=source_file, remote_file=remote_file)
        extract_tarball(cluster=cluster, targets=cluster.get_all_hosts(),
                        remote_file=remote_file,
                        target_folder=cluster.get_tez_install_dir(),
                        strip_level=0)


@task
def make_hdfs_dirs(cluster):
    """ Creates NameNode and DataNode directories."""
    hdfs_master_config = cluster.get_hdfs_master_config()
    for d in hdfs_master_config.get_nn_dirs():
        sudo('install -d -m 0755 {}'.format(d))
        sudo('chown -R hdfs:hadoop {}'.format(d))
    for d in hdfs_master_config.get_snn_dirs():
        sudo('install -d -m 0755 {}'.format(d))
        sudo('chown -R hdfs:hadoop {}'.format(d))
    for d in hdfs_master_config.get_jn_dirs():
        sudo('install -d -m 0755 {}'.format(d))
        sudo('chown -R hdfs:hadoop {}'.format(d))
    for d in cluster.get_datanode_dirs():
        sudo('install -d -m 0700 {}'.format(d))
        sudo('chown -R hdfs:hadoop {}'.format(d))
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        sudo('install -d -m 0755 {}'.format(cluster.get_config(constants.KEY_OZONE_METADIR)))
        sudo('chown -R hdfs:hadoop {}'.format(cluster.get_config(constants.KEY_OZONE_METADIR)))
    # Ensure that the hdfs user has permissions to reach its storage
    # directories.
    for d in cluster.get_hdfs_master_config().get_nn_dirs() + \
            cluster.get_datanode_dirs() + \
            cluster.get_hdfs_master_config().get_snn_dirs():
        while os.path.dirname(d) != d:
            d = os.path.dirname(d)
            sudo('chmod 755 {}'.format(d))
    return True


def copy_jscsi_helper(cluster):
    """ Copy the jscsi helper script to datanodes if ozone is enabled."""
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        install_dir = cluster.get_hadoop_install_dir()
        put('scripts/helper.sh', '%s/bin/helper.sh' % install_dir, use_sudo=True)
        sudo('chmod 755 ' + '%s/bin/helper.sh' % install_dir)
    return True


@task
def create_ozone_metadata_paths(cluster):
    """"Creates Ozone metadata paths. """
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        sudo('mkdir -p %s' % cluster.get_config(constants.KEY_OZONE_METADIR),
             user=constants.HDFS_USER)
        sudo('mkdir -p %s' % os.path.dirname(cluster.get_config(constants.KEY_SCM_DATANODE_ID)),
             user=constants.HDFS_USER)
        sudo('mkdir -p %s' % cluster.get_config(constants.KEY_CBLOCK_CACHE_PATH),
             user=constants.HDFS_USER)
        sudo('mkdir -p %s' % cluster.get_config(constants.KEY_OZONE_METADIR),
             user=constants.HDFS_USER)
    return True


def format_namenode(cluster, cluster_id):
    """ formats a namenode using the given cluster_id.
    """
    # This command will prompt the user, so we are skipping the prompt.
    get_logger().info('Formatting NameNode {}'.format(env.host_string))
    with hide("stdout"):
        return sudo('{}/bin/hdfs namenode -format -clusterid {}'.format(
            cluster.get_hadoop_install_dir(), cluster_id), user=constants.HDFS_USER).succeeded


def format_hdfs_nameservices(cluster, cluster_id):
    """
    Run steps to format an HDFS HA cluster.
        1. Start JournalNodes.
        2. Format active NameNodes.
        3. Start active NameNodes.
        4. Boostrap standby NameNodes.
        5. Shutdown active NameNodes.
        6. Shutdown JournalNodes.
    :param cluster_id:
    :param cluster:
    :return:
    """
    start_stop_all_journalnodes(cluster, action='start')
    # Format and start the active NNs.
    actives = cluster.get_hdfs_master_config().choose_active_nns()
    if not execute(format_namenode, hosts=actives, cluster=cluster, cluster_id=cluster_id):
        get_logger().error("Failed to format one or more active NameNodes.")
        return False
    if not execute(start_stop_service, hosts=actives, cluster=cluster,
                   action='start', service_name='namenode', user=constants.HDFS_USER):
        get_logger().error("Failed to start one or more active NameNodes.")
        return False

    do_sleep(10)

    # Bootstrap the standby NNs.
    standbys = cluster.get_hdfs_master_config().choose_standby_nns()
    if standbys:
        if not execute(bootstrap_standby, hosts=standbys, cluster=cluster):
            get_logger().error("Failed to bootstrap standby NameNodes.")
            return False
        execute(start_stop_service, hosts=standbys, cluster=cluster,
                action='start', service_name='namenode', user=constants.HDFS_USER)
        do_sleep(10)

    # While the NNs are up, let's create tmp directories necessary for
    # running YARN jobs. This works without DataNodes as there are no files/blocks in
    # the system, so NN exits safe mode without restarting the DataNode.
    do_active_transitions(cluster)
    make_hdfs_dir(cluster, '/tmp', '1777')
    make_hdfs_dir(cluster, '/apps', '755')
    make_hdfs_dir(cluster, '/home', '755')
    make_home_dirs(cluster)


@task
def bootstrap_standby(cluster):
    """ Bootstraps a standby NameNode """
    install_dir = cluster.get_hadoop_install_dir()
    get_logger().info("Bootstrapping standby NameNode: {}".format(env.host_string))
    cmd = '{}/bin/hdfs namenode -bootstrapstandby'.format(install_dir)
    return sudo(cmd, user=constants.HDFS_USER).succeeded


def extract_tarball(cluster=None, targets=None, remote_file=None,
                    target_folder=None, strip_level=None):
    get_logger().info("Extracting {} on all nodes".format(remote_file))
    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        if not execute(do_untar, hosts=targets, tarball=remote_file,
                       target_folder=target_folder, strip_level=strip_level):
            get_logger().error("Failed to untar {}".format(remote_file))
            return False


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
def make_hadoop_log_dirs(cluster=None):
    logging_root = os.path.join(cluster.get_hadoop_install_dir(), constants.HADOOP_LOG_DIR_NAME)
    get_logger().debug("Creating log output dir {} on host {}".format(logging_root, env.host))
    sudo('mkdir -p {}'.format(logging_root))
    sudo('chgrp {} {}'.format(constants.HADOOP_GROUP, logging_root))
    sudo('chmod 775 {}'.format(logging_root))


def make_hdfs_dir(cluster, path, perm, owner=constants.HDFS_USER):
    """
    Create the specified directory in each HDFS namespace.
    """
    get_logger().debug("Creating HDFS directory {}".format(path))
    if len(cluster.get_hdfs_master_config().get_nameservices()) == 1:
        # Single namespace, so don't specify it explicitly. It may be a
        # pseudo-namespace for non-HA, non-federated cluster.
        cmd = 'hadoop fs -mkdir -p {0} && hadoop fs -chmod {1} {0} && hadoop fs -chown {2} {0}'.format(path, perm, owner)
        run_dfs_command(cluster=cluster, cmd=cmd)
        return

    for ns in cluster.get_hdfs_master_config().get_nameservices():
        cmd = 'hadoop fs -mkdir -p {0}{1} && hadoop fs -chmod {2} {0}{1} && hadoop fs -chown {3} {0}{1}'.format(
            ns.get_uri(), path, perm, owner)
        run_dfs_command(cluster=cluster, cmd=cmd)


def make_home_dirs(cluster):
    """
    Create a homedir for service users in each namespace.
    :param cluster:
    :return:
    """
    for user in cluster.get_service_users():
        make_hdfs_dir(cluster, '/home/{}'.format(user.name), 700, owner=user.name)


def deploy_tez(cluster):
    """
    Run steps to deploy Apache Tez on the cluster.
    """
    result = execute(run_dfs_command, cluster=cluster,
                     cmd='hadoop fs -mkdir -p /apps/{0} && hadoop fs -chmod 755 /apps && '
                         'hadoop fs -put {1} /apps/{0} && '
                         'hadoop fs -chown -R tez /apps/{0} && hadoop fs -chgrp -R hadoop /apps/{0}'.format(
                            cluster.get_tez_distro_name(),
                            get_tarball_destination(cluster.get_config(constants.KEY_TEZ_TARBALL))))


if __name__ == '__main__':
    pass
