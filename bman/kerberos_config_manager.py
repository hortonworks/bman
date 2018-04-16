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

"""
Kerberos configuration manager for bman.

This module can be used to generate sensible defaults for most Hadoop
security settings.
"""
import os

from bman.constants import HDFS_USER, HADOOP_GROUP, YARN_USER
from bman.exceptions import *

NN_PRINCIPAL_KEY = 'dfs.namenode.kerberos.principal'
NN_KEYTAB_KEY = 'dfs.namenode.keytab.file'
SNN_PRINCIPAL_KEY = 'dfs.secondary.namenode.kerberos.principal'
SNN_KEYTAB_KEY = 'dfs.secondary.namenode.keytab.file'
DN_PRINCIPAL_KEY = 'dfs.datanode.kerberos.principal'
DN_KEYTAB_KEY = 'dfs.datanode.keytab.file'
JN_PRINCIPAL_KEY = 'dfs.journalnode.kerberos.principal'
JN_KEYTAB_KEY = 'dfs.journalnode.keytab.file'
WEB_AUTH_PRINCIPAL_KEY = 'dfs.web.authentication.kerberos.principal'
HTTP_KEYTAB_FILE = 'dfs.web.authentication.kerberos.keytab'

NN_INTERNAL_SPNEGO_PRINCIPAL_KEY = 'dfs.namenode.kerberos.internal.spnego.principal'
JN_INTERNAL_SPNEGO_PRINCIPAL_KEY = 'dfs.journalnode.kerberos.internal.spnego.principal'
SNN_INTERNAL_SPNEGO_PRINCIPAL_KEY = 'dfs.secondary.namenode.kerberos.internal.spnego.principal'

RM_PRINCIPAL_KEY = 'yarn.resourcemanager.principal'
RM_KEYTAB_KEY = 'yarn.resourcemanager.keytab'

NM_PRINCIPAL_KEY = 'yarn.nodemanager.principal'
NM_KEYTAB_KEY = 'yarn.nodemanager.keytab'

DFS_BLOCK_TOKENS_ENABLED_KEY = 'dfs.block.access.token.enable'

# For starting DataNode with privileged resources.
DN_ADDRESS_KEY = 'dfs.datanode.address'
DN_ADDRESS_VALUE = '0.0.0.0:1004'
DN_HTTP_ADDRESS_KEY = 'dfs.datanode.http.address'
DN_HTTP_ADDRESS_VALUE = '0.0.0.0:1016'

KEYTABS_DEFAULT_DIR = '/etc/security/keytabs'


class KerberosConfigGenerator(object):
    """
    This class can generate sane default configuration values for
    Hadoop service principal and keytab files in a secure cluster.
    This reduces configuration burden for administrators.

    These settings are required during the kerberos setup step, and
    also when generating cluster configuration files.
    """
    def __init__(self):
        self.realm = None

    def add_missing_confs(self, realm, values, hdfs_site_values, yarn_site_values):
        """
        Accepts a map of key-value pairs corresponding to Hadoop
        site settings and updates it with default values of
        service principals and keytab files.
        """
        self.realm = realm
        self.add_hdfs_secure_configs(values, hdfs_site_values)
        self.add_yarn_secure_configs(values, yarn_site_values)

    def add_hdfs_secure_configs(self, values, hdfs_site_values):
        if NN_PRINCIPAL_KEY not in values:
            hdfs_site_values[NN_PRINCIPAL_KEY] = \
                values[NN_PRINCIPAL_KEY] = 'nn/_HOST@{}'.format(self.realm)
        if NN_KEYTAB_KEY not in values:
            hdfs_site_values[NN_KEYTAB_KEY] = \
                values[NN_KEYTAB_KEY] = os.path.join(KEYTABS_DEFAULT_DIR, 'nn.service.keytab')

        if DN_PRINCIPAL_KEY not in values:
            hdfs_site_values[DN_PRINCIPAL_KEY] = \
                values[DN_PRINCIPAL_KEY] = 'dn/_HOST@{}'.format(self.realm)
        if DN_KEYTAB_KEY not in values:
            hdfs_site_values[DN_KEYTAB_KEY] = \
                values[DN_KEYTAB_KEY] = os.path.join(KEYTABS_DEFAULT_DIR, 'dn.service.keytab')

        if SNN_PRINCIPAL_KEY not in values:
            hdfs_site_values[SNN_PRINCIPAL_KEY] = \
                values[SNN_PRINCIPAL_KEY] = 'snn/_HOST@{}'.format(self.realm)
        if SNN_KEYTAB_KEY not in values:
            hdfs_site_values[SNN_KEYTAB_KEY] = \
                values[SNN_KEYTAB_KEY] = os.path.join(KEYTABS_DEFAULT_DIR, 'snn.service.keytab')

        if JN_PRINCIPAL_KEY not in values:
            hdfs_site_values[JN_PRINCIPAL_KEY] = \
                values[JN_PRINCIPAL_KEY] = 'jn/_HOST@{}'.format(self.realm)
        if JN_KEYTAB_KEY not in values:
            hdfs_site_values[JN_KEYTAB_KEY] = \
                values[JN_KEYTAB_KEY] = os.path.join(KEYTABS_DEFAULT_DIR, 'jn.service.keytab')

        if WEB_AUTH_PRINCIPAL_KEY not in values:
            hdfs_site_values[WEB_AUTH_PRINCIPAL_KEY] = \
                values[WEB_AUTH_PRINCIPAL_KEY] = 'HTTP/_HOST@{}'.format(self.realm)
        if HTTP_KEYTAB_FILE not in values:
            hdfs_site_values[HTTP_KEYTAB_FILE] = \
                values[HTTP_KEYTAB_FILE] = os.path.join(KEYTABS_DEFAULT_DIR, 'spnego.service.keytab')

        # For the internal spnego principals, make sure we select the same values
        # as the WEB_AUTH_PRINCIPAL_KEY.
        if NN_INTERNAL_SPNEGO_PRINCIPAL_KEY not in values:
            hdfs_site_values[NN_INTERNAL_SPNEGO_PRINCIPAL_KEY] = \
                values[NN_INTERNAL_SPNEGO_PRINCIPAL_KEY] = values[WEB_AUTH_PRINCIPAL_KEY]
        if SNN_INTERNAL_SPNEGO_PRINCIPAL_KEY not in values:
            hdfs_site_values[SNN_INTERNAL_SPNEGO_PRINCIPAL_KEY] = \
                values[SNN_INTERNAL_SPNEGO_PRINCIPAL_KEY] = values[WEB_AUTH_PRINCIPAL_KEY]
        if JN_INTERNAL_SPNEGO_PRINCIPAL_KEY not in values:
            hdfs_site_values[JN_INTERNAL_SPNEGO_PRINCIPAL_KEY] = \
                values[JN_INTERNAL_SPNEGO_PRINCIPAL_KEY] = values[WEB_AUTH_PRINCIPAL_KEY]

        # Kerberos requires block tokens to be enabled.
        hdfs_site_values[DFS_BLOCK_TOKENS_ENABLED_KEY] = \
            values[DFS_BLOCK_TOKENS_ENABLED_KEY] = 'true'

        # Ensure that the Datanode is configured to start with privileged resources.
        # TODO: Support starting the Datanode with SASL.
        if DN_ADDRESS_KEY not in values:
            hdfs_site_values[DN_ADDRESS_KEY] = \
                values[DN_ADDRESS_KEY] = DN_ADDRESS_VALUE
        if DN_HTTP_ADDRESS_KEY not in values:
            hdfs_site_values[DN_HTTP_ADDRESS_KEY] = \
                values[DN_HTTP_ADDRESS_KEY] = DN_HTTP_ADDRESS_VALUE

    def add_yarn_secure_configs(self, values, yarn_site_values):
        if RM_PRINCIPAL_KEY not in values:
            yarn_site_values[RM_PRINCIPAL_KEY] = \
                values[RM_PRINCIPAL_KEY] = 'rm/_HOST@{}'.format(self.realm)
        if RM_KEYTAB_KEY not in values:
            yarn_site_values[RM_KEYTAB_KEY] = \
                values[RM_KEYTAB_KEY] = os.path.join(KEYTABS_DEFAULT_DIR, 'rm.service.keytab')

        if NM_PRINCIPAL_KEY not in values:
            yarn_site_values[NM_PRINCIPAL_KEY] = \
                values[NM_PRINCIPAL_KEY] = 'nm/_HOST@{}'.format(self.realm)
        if NM_KEYTAB_KEY not in values:
            yarn_site_values[NM_KEYTAB_KEY] = \
                values[NM_KEYTAB_KEY] = os.path.join(KEYTABS_DEFAULT_DIR, 'nm.service.keytab')


class PrincipalConfigEntry(object):
    """
    Class that encapsulates principal name and keytab configurations
    for a single principal.
    """

    def __init__(self, principal_key=None, keytab_file_key=None, hosts=None,
                 keytab_file_owner=None, keytab_file_group=None, keytab_perms=None):
        self.principal_key = principal_key
        self.keytab_file_key = keytab_file_key
        self.hosts = hosts
        self.keytab_file_owner = keytab_file_owner
        self.keytab_file_group = keytab_file_group
        self.keytab_perms = keytab_perms


def make_principal_configuration(cluster=None):
    """
    Setup principal and keytab configs for all the services we know about.
    :param cluster:
    :return:
    """
    validate_spnego_principals(cluster)
    pc = [PrincipalConfigEntry(principal_key=NN_PRINCIPAL_KEY,
                               keytab_file_key=NN_KEYTAB_KEY,
                               hosts=cluster.get_hdfs_master_config().get_nn_hosts(),
                               keytab_file_owner=HDFS_USER,
                               keytab_file_group=HADOOP_GROUP,
                               keytab_perms='600'),
          PrincipalConfigEntry(principal_key=SNN_PRINCIPAL_KEY,
                               keytab_file_key=SNN_KEYTAB_KEY,
                               hosts=cluster.get_hdfs_master_config().get_snn_hosts(),
                               keytab_file_owner=HDFS_USER,
                               keytab_file_group=HADOOP_GROUP,
                               keytab_perms='600'),
          PrincipalConfigEntry(principal_key=DN_PRINCIPAL_KEY,
                               keytab_file_key=DN_KEYTAB_KEY,
                               hosts=cluster.get_worker_nodes(),
                               keytab_file_owner=HDFS_USER,
                               keytab_file_group=HADOOP_GROUP,
                               keytab_perms='600'),
          PrincipalConfigEntry(principal_key=JN_PRINCIPAL_KEY,
                               keytab_file_key=JN_KEYTAB_KEY,
                               hosts=cluster.get_hdfs_master_config().get_jn_hosts(),
                               keytab_file_owner=HDFS_USER,
                               keytab_file_group=HADOOP_GROUP,
                               keytab_perms='600'),
          PrincipalConfigEntry(principal_key=WEB_AUTH_PRINCIPAL_KEY,
                               keytab_file_key=HTTP_KEYTAB_FILE,
                               hosts=cluster.get_all_hosts(),
                               keytab_file_owner=HDFS_USER,
                               keytab_file_group=HADOOP_GROUP,
                               keytab_perms='644'),
          PrincipalConfigEntry(principal_key=RM_PRINCIPAL_KEY,
                               keytab_file_key=RM_KEYTAB_KEY,
                               hosts=cluster.get_rm_hosts(),
                               keytab_file_owner=YARN_USER,
                               keytab_file_group=HADOOP_GROUP,
                               keytab_perms='600'),
          PrincipalConfigEntry(principal_key=NM_PRINCIPAL_KEY,
                               keytab_file_key=NM_KEYTAB_KEY,
                               hosts=cluster.get_worker_nodes(),
                               keytab_file_owner=YARN_USER,
                               keytab_file_group=HADOOP_GROUP,
                               keytab_perms='600')]
    return pc


def validate_spnego_principals(cluster=None):
    """
    We currently don't support different values for the various SPNEGO principals.
    TODO: Remove this limitation.
    :param cluster:
    :return:
    """
    spnego_principals = set()
    spnego_principal_keys = {WEB_AUTH_PRINCIPAL_KEY,
                             NN_INTERNAL_SPNEGO_PRINCIPAL_KEY,
                             JN_INTERNAL_SPNEGO_PRINCIPAL_KEY,
                             SNN_INTERNAL_SPNEGO_PRINCIPAL_KEY}

    for spk in spnego_principal_keys:
        if cluster.has_site_setting(spk):
            spnego_principals.add(cluster.get_site_setting(spk))

    if len(spnego_principals) > 1:
        raise KerberosConfigError("Unsupported configuration: The following settings should " +
                                  "have the same value: {}".format(spnego_principal_keys))

