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
import re
import shutil
from string import Template

from fabric.api import hide, execute, sudo, put
from fabric.decorators import task, parallel
from fabric.state import env
from pkg_resources import resource_string, resource_listdir

import bman.constants as constants
from bman.local_tasks import generate_site_config
from bman.logger import get_logger
from bman.remote_tasks import do_active_transitions
from bman.utils import start_stop_service, run_dfs_command, do_sleep, \
    get_tarball_destination, put_to_all_nodes, extract_tarball

"""
This module executes steps to deploy HDFS and YARN services on cluster nodes.

The entry point is the method do_hadoop_install.
"""

def do_hadoop_install(cluster, cluster_id=None):
    if not cluster.is_hadoop_enabled():
        return

    __deploy_hadoop_tarball(cluster=cluster)
    __make_hdfs_storage_directories(cluster)
    __generate_hadoop_configs(cluster=cluster)
    __deploy_common_config_files(cluster)
    __format_hdfs_nameservices(cluster, cluster_id)

    if not execute(start_stop_service, hosts=cluster.get_worker_nodes(), cluster=cluster,
                   action='start', service_name='datanode'):
        get_logger().error("Failed to start one or more DataNodes.")
        return False

    get_logger().info("Done deploying Hadoop to {} nodes.".format(len(cluster.get_all_hosts())))


def __make_hdfs_storage_directories(cluster):
    """
    Make HDFS dirs on DNs, NNs, SNNs and JNs.
    :param cluster:
    :param targets:
    :return:
    """
    get_logger().info("Creating HDFS metadata and data directories")
    master_config = cluster.get_hdfs_configs()
    service_dir_map = {
        "DN": [cluster.get_worker_nodes(), cluster.get_datanode_dirs()],
        "NN": [master_config.get_nn_hosts(), master_config.get_nn_dirs()],
        "SNN": [master_config.get_snn_hosts(), master_config.get_snn_dirs()],
        "JN": [master_config.get_jn_hosts(), master_config.get_jn_dirs()]
    }

    with hide('status', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands'):
        for service, nodes_and_dirs in service_dir_map.items():
            if nodes_and_dirs[0] and nodes_and_dirs[1]:
                get_logger().info("Creating {} directories on {} hosts".format(
                    service, len(nodes_and_dirs[0])))
                if not execute(make_hdfs_storage_dirs,
                    hosts=list(nodes_and_dirs[0]), dirs=nodes_and_dirs[1]):
                    get_logger().error("Failed to make {} directories {}".format(
                        service, nodes_and_dirs[1]))
                    return False


@task
@parallel
def make_hdfs_storage_dirs(dirs):
    """ Creates HDFS directories """
    for d in dirs:
        sudo('install -d -m 0700 {}'.format(d))
        sudo('chown -R hdfs:hadoop {}'.format(d))

        # Ensure that the HDFS user can traverse each parent dir.
        while os.path.dirname(d) != d:
            d = os.path.dirname(d)
            sudo('chmod a+rx {}'.format(d))
    return True


def __deploy_hadoop_tarball(cluster):
    """ Copy the Hadoop Tarball to all nodes and extract it. """
    source_file = cluster.get_config(constants.KEY_HADOOP_TARBALL)
    remote_file = get_tarball_destination(source_file)
    put_to_all_nodes(cluster=cluster, source_file=source_file, remote_file=remote_file)
    # The Hadoop tarball has an extra top-level directory, strip it out.
    extract_tarball(targets=cluster.get_all_hosts(),
                    remote_file=remote_file,
                    target_folder=cluster.get_hadoop_install_dir(),
                    strip_level=1)


def __generate_hadoop_configs(cluster):
    output_dir = cluster.get_generated_hadoop_conf_tmp_dir()
    if cluster.is_hadoop_enabled():
        get_logger().info('Generating core-site.xml to {}.'.format(output_dir))
        generate_site_config(cluster, filename='core-site.xml',
                             settings_key=constants.KEY_CORE_SITE_SETTINGS,
                             output_dir=output_dir)

    if cluster.is_hdfs_enabled():
        get_logger().info('Generating hdfs-site.xml to {}.'.format(output_dir))
        __update_hdfs_configs(cluster)
        generate_site_config(cluster, filename='hdfs-site.xml',
                             settings_key=constants.KEY_HDFS_SITE_SETTINGS,
                             output_dir=output_dir)

    if cluster.is_yarn_enabled():
        get_logger().info('Generating yarn-site.xml to {}.'.format(output_dir))
        __update_yarn_configs(cluster)
        __update_mapred_configs(cluster)
        generate_site_config(cluster, filename='yarn-site.xml',
                             settings_key=constants.KEY_YARN_SITE_SETTINGS,
                             output_dir=cluster.get_generated_hadoop_conf_tmp_dir())
        get_logger().info('Generating mapred-site.xml to {}.'.format(output_dir))
        generate_site_config(cluster, filename='mapred-site.xml',
                             settings_key=constants.KEY_MAPRED_SITE_SETTINGS,
                             output_dir=output_dir)

    targets = cluster.get_all_hosts()
    get_logger().info('copying config files to remote machines.')
    if not execute(__copy_hadoop_config_files, hosts=targets, cluster=cluster,
                   source_dir=output_dir):
        get_logger().error('copying config files failed.')
        return False


@task
def __copy_hadoop_config_files(cluster, source_dir):
    """ Copy the config to the right location."""
    for config_file in glob.glob(os.path.join(source_dir, "*")):
        filename = os.path.basename(config_file)
        full_file_name = os.path.join(cluster.get_hadoop_conf_dir(), filename)
        put(config_file, full_file_name, use_sudo=True)


def __format_hdfs_nameservices(cluster, cluster_id):
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
    __start_stop_all_journalnodes(cluster, action='start')
    # Format and start the active NNs.
    actives = cluster.get_hdfs_configs().choose_active_nns()
    if not execute(__format_namenode, hosts=actives, cluster=cluster, cluster_id=cluster_id):
        get_logger().error("Failed to format one or more active NameNodes.")
        return False
    if not execute(start_stop_service, hosts=actives, cluster=cluster,
                   action='start', service_name='namenode', user=constants.HDFS_USER):
        get_logger().error("Failed to start one or more active NameNodes.")
        return False

    do_sleep(10)

    # Bootstrap the standby NNs.
    standbys = cluster.get_hdfs_configs().choose_standby_nns()
    if standbys:
        if not execute(__bootstrap_standby, hosts=standbys, cluster=cluster):
            get_logger().error("Failed to bootstrap standby NameNodes.")
            return False
        execute(start_stop_service, hosts=standbys, cluster=cluster,
                action='start', service_name='namenode', user=constants.HDFS_USER)
        do_sleep(10)

    # While the NNs are up, let's create tmp directories necessary for
    # running YARN jobs. This works without DataNodes as there are no files/blocks in
    # the system, so NN exits safe mode without restarting the DataNode.
    do_active_transitions(cluster)
    __make_hdfs_dir(cluster, '/tmp', '1777')
    __make_hdfs_dir(cluster, '/apps', '755')
    __make_hdfs_dir(cluster, '/home', '755')
    __make_service_user_home_dirs(cluster)


def __format_namenode(cluster, cluster_id):
    """ formats a namenode using the given cluster_id.
    """
    # This command will prompt the user, so we are skipping the prompt.
    get_logger().info('Formatting NameNode {}'.format(env.host_string))
    with hide("stdout"):
        return sudo('{}/bin/hdfs namenode -format -clusterid {}'.format(
            cluster.get_hadoop_install_dir(), cluster_id), user=constants.HDFS_USER).succeeded


@task
def __bootstrap_standby(cluster):
    """ Bootstraps a standby NameNode """
    install_dir = cluster.get_hadoop_install_dir()
    get_logger().info("Bootstrapping standby NameNode: {}".format(env.host_string))
    cmd = '{}/bin/hdfs namenode -bootstrapstandby'.format(install_dir)
    return sudo(cmd, user=constants.HDFS_USER).succeeded


def __make_service_user_home_dirs(cluster):
    """
    Create a homedir for service users in each namespace.
    :param cluster:
    :return:
    """
    for user in cluster.get_service_users():
        __make_hdfs_dir(cluster, '/home/{}'.format(user.name), 700, owner=user.name)


def __make_hdfs_dir(cluster, path, perm, owner=constants.HDFS_USER):
    """
    Create the specified directory in each HDFS namespace.
    """
    get_logger().debug("Creating HDFS directory {}".format(path))
    if len(cluster.get_hdfs_configs().get_nameservices()) == 1:
        # Single namespace, so don't specify it explicitly. It may be a
        # pseudo-namespace for non-HA, non-federated cluster.
        cmd = 'hadoop fs -mkdir -p {0} && hadoop fs -chmod {1} {0} && hadoop fs -chown {2} {0}'.format(path, perm, owner)
        run_dfs_command(cluster=cluster, cmd=cmd)
        return

    for ns in cluster.get_hdfs_configs().get_nameservices():
        cmd = 'hadoop fs -mkdir -p {0}{1} && hadoop fs -chmod {2} {0}{1} && hadoop fs -chown {3} {0}{1}'.format(
            ns.get_uri(), path, perm, owner)
        run_dfs_command(cluster=cluster, cmd=cmd)


def __start_stop_all_journalnodes(cluster, action=None):
    hdfs_master_config = cluster.get_hdfs_configs()
    targets = hdfs_master_config.get_jn_hosts()
    if targets:
        return execute(start_stop_service, hosts=targets, cluster=cluster,
                       action=action, service_name='journalnode',
                       user=constants.HDFS_USER)


def __update_mapred_configs(cluster):
    """
    Add missing mapred-site.xml configuration settings that are required by YARN.

    This reduces administrative burden by adding sensible defaults for some mandatory
    settings.
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


def __update_yarn_configs(cluster):
    """
    Add missing yarn-site.xml configuration settings that are required by YARN.
    See the Apache docs for yarn-default.xml for a description of these settings:
    https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-common/yarn-default.xml

    This reduces administrative burden by adding sensible defaults for some mandatory
    settings.
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


def __update_hdfs_configs(cluster):
    """
    Add missing hdfs-site.xml configuration settings that are required by HDFS.

    This reduces administrative burden by adding sensible defaults for some mandatory
    settings.
    """
    settings_dict = cluster.get_config(constants.KEY_HDFS_SITE_SETTINGS)

    for ns in cluster.get_hdfs_configs().get_nameservices():
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
        get_logger().debug("Copying {} to {}:{}".format(src, env.host, cluster.get_hadoop_conf_dir()))
        put(src, os.path.join(cluster.get_hadoop_conf_dir(), f), use_sudo=True)


if __name__ == '__main__':
    pass
