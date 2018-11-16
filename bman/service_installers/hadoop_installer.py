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

from fabric.api import hide, execute, sudo, put
from fabric.decorators import task
from fabric.state import env

import bman.constants as constants
from bman.local_tasks import generate_configs, generate_site_config
from bman.logger import get_logger
from bman.remote_tasks import do_active_transitions
from bman.utils import start_stop_service, run_dfs_command, do_sleep, \
    get_tarball_destination, put_to_all_nodes, extract_tarball


def make_hdfs_storage_directories(cluster=None):
    """
    Make the NameNode and DataNode directories.
    :param cluster:
    :param targets:
    :return:
    """
    targets = cluster.get_all_hosts()
    get_logger().info("Creating HDFS metadata and data directories")
    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        if not execute(task_make_hdfs_dirs, hosts=targets, cluster=cluster):
            get_logger().error("Failed to make HDFS directories")
            return False


def do_hadoop_install(cluster=None, cluster_id=None):
    deploy_hadoop_tarball(cluster=cluster)
    generate_hadoop_configs(cluster=cluster)
    make_hdfs_storage_directories(cluster)
    format_hdfs_nameservices(cluster, cluster_id)

    if not execute(start_stop_service, hosts=cluster.get_worker_nodes(), cluster=cluster,
                   action='start', service_name='datanode'):
        get_logger().error("Failed to start one or more DataNodes.")
        return False

    get_logger().info("Done deploying Hadoop to {} nodes.".format(len(cluster.get_all_hosts())))


@task
def task_make_hdfs_dirs(cluster):
    """ Creates NameNode and DataNode directories."""

    #TODO: Only create DN dirs on DN hosts and NN dirs on NN nodes.
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
    # Ensure that the hdfs user has permissions to reach its storage
    # directories.
    for d in cluster.get_hdfs_master_config().get_nn_dirs() + \
            cluster.get_datanode_dirs() + \
            cluster.get_hdfs_master_config().get_snn_dirs():
        while os.path.dirname(d) != d:
            d = os.path.dirname(d)
            sudo('chmod 755 {}'.format(d))
    return True


def deploy_hadoop_tarball(cluster=None):
    source_file = cluster.get_config(constants.KEY_HADOOP_TARBALL)
    remote_file = get_tarball_destination(source_file)
    put_to_all_nodes(cluster=cluster, source_file=source_file, remote_file=remote_file)
    # The Hadoop tarball has an extra top-level directory, strip it out.
    extract_tarball(targets=cluster.get_all_hosts(),
                    remote_file=remote_file,
                    target_folder=cluster.get_hadoop_install_dir(),
                    strip_level=1)


def generate_hadoop_configs(cluster=None):
    output_dir = cluster.get_generated_hadoop_conf_tmp_dir()
    if cluster.is_hadoop_enabled():
        generate_site_config(cluster, filename='core-site.xml',
                             settings_key=constants.KEY_CORE_SITE_SETTINGS,
                             output_dir=output_dir)

    if cluster.is_hdfs_enabled():
        update_hdfs_configs(cluster)
        generate_site_config(cluster, filename='hdfs-site.xml',
                             settings_key=constants.KEY_HDFS_SITE_SETTINGS,
                             output_dir=output_dir)

    if cluster.is_yarn_enabled():
        update_mapred_configs(cluster)
        update_yarn_configs(cluster)
        generate_site_config(cluster, filename='yarn-site.xml',
                             settings_key=constants.KEY_YARN_SITE_SETTINGS,
                             output_dir=cluster.get_generated_hadoop_conf_tmp_dir())
        generate_site_config(cluster, filename='mapred-site.xml',
                             settings_key=constants.KEY_MAPRED_SITE_SETTINGS,
                             output_dir=output_dir)

    targets = cluster.get_all_hosts()
    get_logger().info('copying config files to remote machines.')
    if not execute(copy_hadoop_config_files, hosts=targets, cluster=cluster,
                   source_dir=output_dir):
        get_logger().error('copying config files failed.')
        return False


@task
def copy_hadoop_config_files(cluster=None, source_dir=None):
    """ Copy the config to the right location."""
    for config_file in glob.glob(os.path.join(source_dir, "*")):
        filename = os.path.basename(config_file)
        full_file_name = os.path.join(cluster.get_hadoop_conf_dir(), filename)
        put(config_file, full_file_name, use_sudo=True)


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


def format_namenode(cluster, cluster_id):
    """ formats a namenode using the given cluster_id.
    """
    # This command will prompt the user, so we are skipping the prompt.
    get_logger().info('Formatting NameNode {}'.format(env.host_string))
    with hide("stdout"):
        return sudo('{}/bin/hdfs namenode -format -clusterid {}'.format(
            cluster.get_hadoop_install_dir(), cluster_id), user=constants.HDFS_USER).succeeded


@task
def bootstrap_standby(cluster):
    """ Bootstraps a standby NameNode """
    install_dir = cluster.get_hadoop_install_dir()
    get_logger().info("Bootstrapping standby NameNode: {}".format(env.host_string))
    cmd = '{}/bin/hdfs namenode -bootstrapstandby'.format(install_dir)
    return sudo(cmd, user=constants.HDFS_USER).succeeded


def make_home_dirs(cluster):
    """
    Create a homedir for service users in each namespace.
    :param cluster:
    :return:
    """
    for user in cluster.get_service_users():
        make_hdfs_dir(cluster, '/home/{}'.format(user.name), 700, owner=user.name)


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


def start_stop_all_journalnodes(cluster, action=None):
    hdfs_master_config = cluster.get_hdfs_master_config()
    targets = hdfs_master_config.get_jn_hosts()
    if targets:
        return execute(start_stop_service, hosts=targets, cluster=cluster,
                       action=action, service_name='journalnode',
                       user=constants.HDFS_USER)

def update_mapred_configs(cluster=None):
    """
    Add missing mapred-site.xml configuration settings that are required by YARN.
    """
    settings_dict = cluster.get_config(constants.KEY_MAPRED_SITE_SETTINGS)

    # Initialize this classpath if it is not already provided in config.yaml.
    if 'mapreduce.application.classpath' not in settings_dict:
        settings_dict['mapreduce.application.classpath'] =\
            "{home}/share/hadoop/mapreduce/*,{home}/share/hadoop/mapreduce/lib/*".format(
                home=cluster.get_hadoop_install_dir())

    if 'mapreduce.framework.name' not in settings_dict:
        settings_dict['mapreduce.framework.name'] = 'yarn-tez' if cluster.is_tez_enabled() else 'yarn'

    if 'mapreduce.app-submission.cross-platform' not in settings_dict:
        settings_dict['mapreduce.app-submission.cross-platform'] = 'false'


def update_yarn_configs(cluster=None):
    """
    Add missing yarn-site.xml configuration settings that are required by YARN.
    See the Apache docs for yarn-default.xml for a description of these settings:
    https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-common/yarn-default.xml
    """
    settings_dict = cluster.get_config(constants.KEY_YARN_SITE_SETTINGS)

    if cluster.is_kerberized():

        if 'yarn.nodemanager.linux-container-executor.group' not in settings_dict:
            settings_dict['yarn.nodemanager.linux-container-executor.group'] = 'hadoop'

        if 'yarn.nodemanager.container-executor.class' not in settings_dict:
            settings_dict['yarn.nodemanager.container-executor.class'] = \
                'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor'

        if 'yarn.nodemanager.linux-container-executor.path' not in settings_dict:
            settings_dict['yarn.nodemanager.linux-container-executor.path'] = \
                os.path.join(cluster.get_hadoop_install_dir(), 'bin', 'container-executor')

    if 'yarn.nodemanager.aux-services' not in settings_dict:
        settings_dict['yarn.nodemanager.aux-services'] = 'mapreduce_shuffle'

    if 'yarn.nodemanager.vmem-check-enabled' not in settings_dict:
        settings_dict['yarn.nodemanager.vmem-check-enabled'] = 'false'


def update_hdfs_configs(cluster):
    """
    Add missing hdfs-site.xml configuration settings that are required by HDFS.

    This reduces administrative burden by adding sensible defaults for some mandatory
    settings.
    """
    settings_dict = cluster.get_config(constants.KEY_HDFS_SITE_SETTINGS)

    for ns in cluster.get_hdfs_master_config().get_nameservices():
        if ns.is_ha():
            fopp_key = 'dfs.client.failover.proxy.provider.{}'.format(ns.get_id())
            if fopp_key not in settings_dict:
                settings_dict[fopp_key] = \
                    'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
            for nn in ns.get_nn_configs():
                http_address_key = 'dfs.namenode.http-address.{}.{}'.format(ns.get_id(), nn.get_id())
                if http_address_key not in settings_dict:
                    settings_dict[http_address_key] = '{}:{}'.format(
                        nn.get_hostname(), constants.DEFAULT_NAMENODE_HTTP_PORT)
        elif ns.need_snn_config:
            settings_dict[ns.snn_host_key] = '{}:{}'.format(
                ns.snn_hosts[0], constants.DEFAULT_SECONDARY_NAMENODE_HTTP_PORT)

    # Enable a few settings that are always useful to have on.
    for setting_key in ['dfs.webhdfs.enabled', 'dfs.disk.balancer.enabled', 'dfs.namenode.acls.enabled']:
        if setting_key not in settings_dict:
            settings_dict[setting_key] = 'true'

    if 'dfs.permissions.superusergroup' not in settings_dict:
        settings_dict['dfs.permissions.superusergroup'] = constants.HADOOP_GROUP

    # Make sure the value of dfs.replication is set sanely for the cluster.
    if 'dfs.replication' not in settings_dict:
        settings_dict['dfs.replication'] = max(3, len(cluster.get_worker_nodes()))


