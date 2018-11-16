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

from fabric.api import execute, sudo, put

import bman.constants as constants
from bman.local_tasks import generate_site_config
from bman.logger import get_logger
from bman.utils import get_tarball_destination, run_dfs_command, put_to_all_nodes, extract_tarball


def do_tez_install(cluster=None):
    if cluster.is_tez_enabled():
        deploy_tez_tarball(cluster=cluster)
        generate_tez_config_files(cluster=cluster)
        deploy_tez(cluster)


def deploy_tez_tarball(cluster=None):
    if cluster.is_tez_enabled():
        source_file = cluster.get_config(constants.KEY_TEZ_TARBALL)
        remote_file = get_tarball_destination(source_file)
        put_to_all_nodes(cluster=cluster, source_file=source_file, remote_file=remote_file)
        extract_tarball(targets=cluster.get_all_hosts(),
                        remote_file=remote_file,
                        target_folder=cluster.get_tez_install_dir(),
                        strip_level=0)

def generate_tez_config_files(cluster=None):
    if cluster.is_tez_enabled():
        update_tez_configs(cluster)
        generate_site_config(cluster, filename='tez-site.xml',
                             settings_key=constants.KEY_TEZ_SITE_SETTINGS,
                             output_dir=cluster.get_generated_tez_conf_tmp_dir())


    targets = cluster.get_all_hosts()
    if cluster.is_tez_enabled() and not execute(copy_tez_config_files, hosts=targets, cluster=cluster):
        get_logger().error('copying config files failed.')
        return False


def copy_tez_config_files(cluster):
    for config_file in glob.glob(os.path.join(cluster.get_generated_tez_conf_tmp_dir(), "*")):
        filename = os.path.basename(config_file)
        full_file_name = os.path.join(cluster.get_tez_conf_dir(), filename)
        sudo('mkdir -p {}'.format(cluster.get_tez_conf_dir()))
        put(config_file, full_file_name, use_sudo=True)


def update_tez_configs(cluster):
    """
    Add missing tez-site.xml configuration settings that are required by Tez.

    This reduces administrative burden by adding sensible defaults for some mandatory
    settings.
    """
    settings_dict = cluster.get_config(constants.KEY_TEZ_SITE_SETTINGS)

    if 'tez.lib.uris' not in settings_dict:
        settings_dict['tez.lib.uris'] = cluster.get_tez_lib_uris_paths()


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
