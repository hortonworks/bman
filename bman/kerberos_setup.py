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

from fabric.contrib.files import exists
from fabric.decorators import task
from fabric.operations import sudo, put
from fabric.state import env
from fabric.tasks import execute
from pkg_resources import resource_filename

from bman.constants import *
from bman.exceptions import *
from bman.kerberos_config_manager import make_principal_configuration
from bman.logger import get_logger
from bman.utils import copy, run_cmd, fast_copy

KEY_KADMIN_SERVER = 'KadminServer'
KEY_KADMIN_PRINCIPAL = 'KadminPrincipal'
KEY_KADMIN_PASSWORD = 'KadminPassword'

HOST_SUBSTITUTION_PATTERN = '_HOST'


class KadminUtil(object):
    """
    Class that encapsulates interaction with the kadmin kadmin_server.
    """

    def __init__(self, cluster):
        self.kadmin_server = cluster.get_config(KEY_KADMIN_SERVER)
        self.kadmin_principal = cluster.get_config(KEY_KADMIN_PRINCIPAL)
        self.kadmin_password = cluster.get_config(KEY_KADMIN_PASSWORD)
        self.cluster = cluster

    def get_get_principal_command(self, principal):
        return 'kadmin -s {} -p {} -w {} get_principal {}'.format(
            self.kadmin_server, self.kadmin_principal, self.kadmin_password, principal)

    def get_create_principal_command(self, principal):
        return 'kadmin -s {} -p {} -w {} add_principal -randkey {}'.format(
            self.kadmin_server, self.kadmin_principal, self.kadmin_password, principal)

    def export_keytab_command(self, principal, keytab_file):
        return 'kadmin -s {} -p {} -w {} ktadd -k {} {}'.format(
            self.kadmin_server, self.kadmin_principal, self.kadmin_password,
            keytab_file, principal)

    def get_default_realm(self):
        return re.sub('^.*@', '', self.cluster.get_site_setting(
            'dfs.namenode.kerberos.principal'))


def make_headless_principals(cluster):
    kadmin_util = KadminUtil(cluster)
    for user in cluster.get_service_users():
        make_headless_principal(cluster, kadmin_util, user)


def do_kerberos_install(cluster=None):

    if not cluster.is_kerberized():
        get_logger().info("Kerberos is not enabled. Skipping Kerberos install.")
        return

    get_logger().info("Enabling Kerberos support.")
    get_logger().info("Installing jsvc and Linux container executor on all cluster hosts")
    copy_jce_policy_files(cluster)
    execute(install_jsvc, hosts=cluster.get_all_hosts())
    execute(install_container_executor, hosts=cluster.get_all_hosts(), cluster=cluster)
    make_headless_principals(cluster)
    generate_hdfs_principals_and_keytabs(cluster=cluster)


def make_headless_principal(cluster=None, kadmin_util=None, user=None):
    """
    Create a headless principal and keytab.
    """
    if not cluster.has_site_setting('dfs.namenode.kerberos.principal'):
        # Not a kerberised cluster, potentially.
        return

    if not kadmin_util:
        kadmin_util = KadminUtil(cluster)

    realm = kadmin_util.get_default_realm()
    keytab_dir = os.path.dirname(cluster.get_site_setting('dfs.namenode.keytab.file'))
    keytab_file = os.path.join(keytab_dir, '{}.headless.keytab'.format(user.name))

    one_nn = cluster.get_hdfs_master_config().get_nn_hosts()[0:1]

    # The headless principal can be created on any host. Just use any one NameNode.
    execute(make_principal, hosts=one_nn,
            kadmin_util=kadmin_util, principal='{}@{}'.format(user.name, realm))

    # Now export a keytab on the same host.
    execute(export_keytab, hosts=one_nn,
            kadmin_util=kadmin_util, principal='{}@{}'.format(user.name, realm),
            keytab_file=keytab_file, keytab_file_owner=user.name,
            keytab_file_group=user.group,
            keytab_perms='600')

    # Now copy the keytab to each remaining host. Don't regenerate the keytab
    # multiple times
    get_logger().info("Distributing {} to all cluster nodes.".format(keytab_file))
    targets = set(cluster.get_all_hosts()).symmetric_difference(one_nn)
    execute(run_cmd, hosts=targets,
            cmd_string='mkdir -p {0} && chmod 755 {0}'.format(keytab_dir))
    execute(fast_copy, hosts=one_nn, cluster=cluster, remote_file=keytab_file)
    execute(run_cmd, hosts=targets,
            cmd_string='chown {0}.{1} {2} && chmod 600 {2}'.format(
                user.name, user.group, keytab_file))


def generate_hdfs_principals_and_keytabs(cluster=None):
    """
    Generate HDFS principals and keytabs on all hosts.
    :param cluster:
    :return:
    """
    kadmin_util = KadminUtil(cluster)
    principal_configs = make_principal_configuration(cluster)

    for pc in principal_configs:
        # Ensure that kerberos config exists for this service.
        if pc.hosts and cluster.has_site_setting(pc.principal_key):
            principal = cluster.get_site_setting(pc.principal_key)
            execute(make_principal, hosts=pc.hosts, kadmin_util=kadmin_util, principal=principal)
            if cluster.has_site_setting(pc.keytab_file_key):
                keytab_file = cluster.get_site_setting(pc.keytab_file_key)
                execute(export_keytab, hosts=pc.hosts, kadmin_util=kadmin_util,
                        principal=principal, keytab_file=keytab_file,
                        keytab_file_owner=pc.keytab_file_owner,
                        keytab_file_group=pc.keytab_file_group,
                        keytab_perms=pc.keytab_perms)


@task
def make_principal(kadmin_util=None, principal=None):
    """
    Create a principal via kadmin, substituting _HOST in the principal name with the current
    hostname.
    """
    principal = principal.replace(HOST_SUBSTITUTION_PATTERN, env.host)

    get_logger().debug(" >> Creating principal {} on host {}".format(principal, env.host))
    cmd = kadmin_util.get_create_principal_command(principal)
    result = sudo(cmd, warn_only=True, quiet=True)
    if result.failed:
        # Failure is okay if the principal already exists.
        result2 = sudo(kadmin_util.get_get_principal_command(principal), warn_only=True)
        if result2.failed:
            raise PrincipalException(result)


@task
def export_keytab(kadmin_util=None, principal=None, keytab_file=None,
                  keytab_file_owner=None, keytab_file_group=None,
                  keytab_perms=None):
    principal = principal.replace(HOST_SUBSTITUTION_PATTERN, env.host)

    keytab_dir = os.path.dirname(keytab_file)
    sudo('mkdir -p {0} && chmod 755 {0}'.format(keytab_dir))
    sudo('rm -f {}'.format(keytab_file))
    get_logger().debug(" >> Exporting keytab {} on host {}".format(keytab_file, env.host))
    cmd = kadmin_util.export_keytab_command(principal, keytab_file)
    sudo(cmd)
    get_logger().debug(" >> Setting keytab owner to {}.{}".format(keytab_file_owner, 'hadoop'))
    sudo('chown {}.{} {}'.format(keytab_file_owner, keytab_file_group, keytab_file))
    sudo('chmod {} {}'.format(keytab_perms, keytab_file))


@task
def install_jsvc():
    jsvc_file = resource_filename('bman.resources.bin', 'jsvc')
    remote_path = os.path.join(JSVC_HOME, 'jsvc')
    sudo('mkdir -p {0} && chmod 755 {0}'.format(JSVC_HOME))
    put(local_path=jsvc_file, remote_path=remote_path)
    sudo('chmod 755 {}'.format(remote_path))


@task
def install_container_executor(cluster=None):
    """
    Install the YARN Linux container executor if it is not already present
    on the node. This uses a bundled binary that should work on most Linux
    distributions.

    It is a fallback to allow enabling Kerberos security with
    an HDP distribution that was compiled without Linux native support.

    The container-executor binary should be setuid.

    :param cluster:
    :return:
    """
    local_ce_file = resource_filename('bman.resources.bin', 'container-executor')
    remote_path = os.path.join(cluster.get_hadoop_install_dir(), 'bin/container-executor')
    if not exists(path=remote_path):
        get_logger().debug(" >> Copying container executor from {} to {}".format(local_ce_file, remote_path))
        put(local_path=local_ce_file, remote_path=remote_path)
    sudo('chown root.{0} {1} && chmod 6050 {1}'.format(
        HADOOP_GROUP, remote_path))


def copy_jce_policy_files(cluster):
    """" Copy JCE unlimited strength policy files to all nodes. """
    source_folder = cluster.get_config(KEY_JCE_POLICY_FILES_LOCATION)
    if not source_folder:
        raise KerberosConfigError(
            'The location of JCE Unlimited Strength Policy files was not found in {}'.format(
                cluster.get_config_file()))

    jar_files_pattern = os.path.join(source_folder, "*.jar")
    if not glob.glob(jar_files_pattern):
        raise KerberosConfigError(
            'No policy jar files found in {}'.format(source_folder))
    target_dir = os.path.join(cluster.get_config(KEY_JAVA_HOME),
                              'jre', 'lib', 'security')
    execute(copy, hosts=cluster.get_all_hosts(),
            source_file=jar_files_pattern, remote_file=target_dir)


