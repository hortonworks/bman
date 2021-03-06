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

import getpass
import os
import pprint
import re
import sys

import yaml

from bman.constants import *
# To add a new config setting, Add a Key in the DEFAULT_CONFIG yaml file.
# Add the correponding key in the CONSTS defined below and then make sure
# you read the value into the config dict in the cluster_constructor function.
#
# if is a setting for hdfs_site.xml, you need to add the template to
# hdfs_site.xml.template and then pass the value to template processing in
# local_tasks.py#generate_hdfs_site
from bman.hdfs_master_configs import HdfsMasterConfigs, ConfigurationError
from bman.kerberos_config_manager import KerberosConfigGenerator
from bman.kerberos_setup import KEY_KADMIN_SERVER, KEY_KADMIN_PRINCIPAL, KEY_KADMIN_PASSWORD
from bman.logger import get_logger


class Cluster(object):
    """
    This class is the logical representation of cluster config that we
    have read from the yaml file.

    TODO convert member variables to Key, it is easier to expand with that
    approach.
    """

    def __init__(self, values, config_file=None):
        self.config = {}
        self.all_site_settings = {}
        self.hdfs_master_configs = None
        self.worker_nodes, self.rm_hosts, = [], []
        self.config_file = config_file
        self.cluster_constructor(values)
        # TODO: Use randomly generated passwords instead. Currently the password
        # is the same as the username.
        self.all_users = {
            HDFS_USER: UserConfig(HDFS_USER, HDFS_USER, HADOOP_GROUP),
            YARN_USER: UserConfig(YARN_USER, YARN_USER, HADOOP_GROUP),
            MAPREDUCE_USER: UserConfig(MAPREDUCE_USER, MAPREDUCE_USER, HADOOP_GROUP),
            TEZ_USER: UserConfig(TEZ_USER, TEZ_USER, HADOOP_GROUP)}

        # TODO: Remove cyclic dependency between bman_config and kerberos_config_manager.
        if self.is_kerberized():
            # Update both - (1) all_site_settings and (2) hdfs site settings.
            # The former is used by bman, the latter is read by the Hadoop services
            # and clients.
            # TODO: Consider consolidating them instead.
            KerberosConfigGenerator().add_missing_confs(
                self.get_config(KEY_REALM), self.all_site_settings,
                self.get_config(KEY_HDFS_SITE_SETTINGS),
                self.get_config(KEY_YARN_SITE_SETTINGS))

        self.dump_node_configuration()

    def __repr__(self):
        return pprint.pformat(self.config, indent=4)

    def get_config(self, key):
        if not key in self.config:
            return None
        return self.config.get(key)

    def has_site_setting(self, key):
        return key in self.all_site_settings and self.all_site_settings[key]

    def get_site_setting(self, key):
        return self.all_site_settings[key]

    def read_config_value_with_default(self, values, key, default_value=None):
        if key in values:
            self.config[key] = values[key]
        else:
            self.config[key] = default_value

    def read_config_value_with_altkey(self, values, key, altkey):
        value = None
        if key in values:
            self.config[key] = value = values[key]
        if not value and altkey in values:
            get_logger().warn("{} has been deprecated by {}. Please update {}".format(
                altkey, key, self.get_config_file()))
            self.config[key] = values[altkey]
        if not value:
            raise ValueError("Required key {} is missing in YAML.".format(key))

    def read_required_config_value(self, values, key):
        value = None
        if key in values:
            self.config[key] = value = values[key]
        if not value:
            raise ValueError("Required key : {} is missing in YAML.".format(key))

    def get_hdfs_master_config(self):
        return self.hdfs_master_configs

    def cluster_constructor(self, values):
        """
        Constructor helper that maps YAML values to cluster object.
        :param values:
        :return:
        """
        # Required config values if this is missing we will raise an exception
        self.read_required_config_value(values, KEY_NAME)
        self.read_config_value_with_altkey(values, KEY_WORKERS, KEY_DATANODES)
        self.read_config_value_with_altkey(values, KEY_HADOOP_TARBALL, KEY_TARBALL)
        self.read_required_config_value(values, KEY_HOMEDIR)
        self.read_required_config_value(values, KEY_CORE_SITE_SETTINGS)
        self.read_required_config_value(values, KEY_HDFS_SITE_SETTINGS)
        self.read_config_value_with_default(values, KEY_OZONE_SITE_SETTINGS, {})

        # Yarn and mapred settings are optional. If absent, then YARN services will
        # not be started.
        self.read_config_value_with_default(values, KEY_YARN_SITE_SETTINGS, {})
        self.read_config_value_with_default(values, KEY_MAPRED_SITE_SETTINGS, {})

        # Tez settings are also optional.  If absent, then Tez will not be installed.
        self.read_config_value_with_default(values, KEY_TEZ_TARBALL, None)
        self.read_config_value_with_default(values, KEY_TEZ_SITE_SETTINGS, {})

        self.all_site_settings = {**self.config[KEY_CORE_SITE_SETTINGS],
                                  **self.config[KEY_HDFS_SITE_SETTINGS],
                                  **self.config[KEY_OZONE_SITE_SETTINGS],
                                  **self.config[KEY_MAPRED_SITE_SETTINGS],
                                  **self.config[KEY_YARN_SITE_SETTINGS],
                                  **self.config[KEY_TEZ_SITE_SETTINGS]}

        self.read_config_value_with_default(values, KEY_JAVA_HOME, DEFAULT_JAVA_HOME)
        self.hdfs_master_configs = HdfsMasterConfigs(self.all_site_settings)
        self.init_host_lists()

        self.read_config_value_with_default(values, KEY_OZONE_ENABLED, False)
        # If ozone is enabled then we must know where to place ozone metadata.
        if self.get_config(KEY_OZONE_ENABLED):
            self.read_required_config_value(values, KEY_OZONE_METADIR)

        # If no value is specified for these two services then use the address of
        # any one NameNode.
        one_nn = sorted(list(self.get_hdfs_master_config().get_nn_hosts()))[0]
        self.read_config_value_with_default(values, KEY_SCMADDRESS, one_nn)
        self.read_config_value_with_default(values, KEY_CBLOCK_ADDRESS, one_nn)

        # Key values with defaults. If user does not specify a value,
        # we will use the defaults.
        self.read_config_value_with_default(values, KEY_FORCE_WIPE, 'False')
        self.read_config_value_with_default(values, KEY_SCM_DATANODE_ID, "/data/disk1/scm/meta/node/datanode.id")
        self.read_config_value_with_default(values, KEY_CBLOCK_CACHE, 'True')
        self.read_config_value_with_default(values, KEY_CBLOCK_TRACE, 'False')
        self.read_config_value_with_default(values, KEY_CBLOCK_CACHE_PATH, '/home/hdfs/cblock_cache')
        self.read_config_value_with_default(values, KEY_USER, getpass.getuser())
        self.read_config_value_with_default(values, KEY_PASSWORD)
        self.read_config_value_with_default(values, KEY_SSH_KEYFILE)

        # Read kadmin server settings.
        self.read_config_value_with_default(values, KEY_KADMIN_SERVER)
        self.read_config_value_with_default(values, KEY_KADMIN_PRINCIPAL)
        self.read_config_value_with_default(values, KEY_KADMIN_PASSWORD)
        self.read_config_value_with_default(values, KEY_JCE_POLICY_FILES_LOCATION)
        self.read_config_value_with_default(values, KEY_REALM)

    def init_host_lists(self):
        """
        Initialize list of NN, DN, JN, RM hostnames for quick access.
        :return:
        """
        self.worker_nodes = self.get_config(KEY_WORKERS)
        if self.is_yarn_enabled():
            self.rm_hosts.append(
                self.get_site_setting('yarn.resourcemanager.address').split(':')[0])

    def get_worker_nodes(self):
        return self.worker_nodes

    def get_rm_hosts(self):
        return self.rm_hosts

    def get_all_hosts(self):
        # Make sure we dedup the final list as there will be multiple
        # components on each host!
        return list(set(self.get_hdfs_master_config().get_nn_hosts() +
                        self.worker_nodes +
                        self.get_hdfs_master_config().get_jn_hosts() +
                        self.rm_hosts))

    def is_kerberized(self):
        return self.has_site_setting('hadoop.security.authentication') and \
            self.get_site_setting('hadoop.security.authentication').lower() == 'kerberos'

    def get_hadoop_install_dir(self):
        return '{}/{}'.format(self.get_config(KEY_HOMEDIR), self.get_hadoop_distro_name())

    def get_tez_install_dir(self):
        return '{}/{}'.format(self.get_config(KEY_HOMEDIR), self.get_tez_distro_name())

    def get_hadoop_distro_name(self):
        tarball_name = os.path.basename(self.get_config(KEY_HADOOP_TARBALL))
        return re.sub(r"(.tgz|.tar.gz|.tar.bz2|.tar.bzip2|tar.Z|.tar.xz)$", "", tarball_name)

    def get_tez_distro_name(self):
        tarball_name = os.path.basename(self.get_config(KEY_TEZ_TARBALL))
        return re.sub(r"(.tgz|.tar.gz|.tar.bz2|.tar.bzip2|tar.Z|.tar.xz)$", "", tarball_name)

    def get_datanode_dirs(self):
        return self.get_site_setting('dfs.datanode.data.dir').split(',')

    def is_yarn_enabled(self):
        return self.get_config(KEY_YARN_SITE_SETTINGS)

    def get_service_user_names(self):
        return self.all_users.keys()

    def get_service_users(self):
        """
        Return the list of users to be created on the cluster.
        TODO: Support custom passwords.
        :return:
        """
        return self.all_users.values()

    def get_user_password(self, username):
        return self.all_users[username].password

    def get_config_file(self):
        return self.config_file

    def get_realm(self):
        return self.get_config(KEY_REALM)

    def get_ssh_keys_tmp_dir(self):
        """
        Get a temporary directory for generating ssh keys
        for this cluster.
        :return:
        """
        return os.path.join(
            os.path.expanduser('~'), '.config', 'bman',
            '.ssh-keys-{}'.format(self.get_config(KEY_NAME)))

    def get_generated_hadoop_conf_tmp_dir(self):
        """
        Get a temporary directory for generating config files
        for this cluster.
        :return:
        """
        return os.path.join(
            os.path.expanduser('~'), '.config', 'bman',
            '.conf-generated-{}-hadoop'.format(self.get_config(KEY_NAME)))

    def get_generated_tez_conf_tmp_dir(self):
        """
        Get a temporary directory for generating config files
        for this cluster.
        :return:
        """
        return os.path.join(
            os.path.expanduser('~'), '.config', 'bman',
            '.conf-generated-{}-tez'.format(self.get_config(KEY_NAME)))

    def get_hadoop_conf_dir(self):
        return os.path.join(self.get_hadoop_install_dir(), 'etc', 'hadoop')

    def get_tez_conf_dir(self):
        return os.path.join(self.get_tez_install_dir(), 'conf')

    def dump_node_configuration(self):
        """
        Write node configuration to debug logs.
        """
        get_logger().debug("NN hosts are {}".format(self.get_hdfs_master_config().get_nn_hosts()))
        get_logger().debug("Worker hosts are {}".format(self.worker_nodes))
        get_logger().debug("JN hosts are {}".format(self.get_hdfs_master_config().get_jn_hosts()))
        get_logger().debug("RM hosts are {}".format(self.rm_hosts))

    def is_tez_enabled(self):
        return self.get_config(KEY_TEZ_TARBALL)

    def get_tez_lib_uris_paths(self):
        default_fs = re.sub('/$', '', self.get_site_setting('fs.defaultFS'))  # Remove trailing '/', if any.
        return '{}/apps/{}/{}'.format(
            default_fs, self.get_tez_distro_name(), os.path.basename(self.get_config(KEY_TEZ_TARBALL)))


def get_default_config_file():
    return os.path.join(os.path.expanduser('~'), '.config', 'bman', 'config.yaml')


def load_config(config_file=None):
    """
    Reads the default YAML Config.
    :param config_file: YAML File to read.
    :return: Cluster class
    """
    if not config_file:
        config_file = get_default_config_file()
    if not os.path.exists(config_file):
        get_logger().error("Error: config file {} does not exist\n".format(config_file))
        sys.exit(1)
    with open(config_file, 'r') as stream:
        cluster = Cluster(yaml.safe_load(stream), config_file)
    return cluster


class UserConfig(object):
    def __init__(self, name, password, group):
        self.name = name
        self.password = password
        self.group = group


if __name__ == '__main__':
    pass
