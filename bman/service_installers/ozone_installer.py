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
import string

from fabric.api import execute
from fabric.decorators import task
from fabric.operations import sudo
from pkg_resources import resource_string

import bman.constants as constants
from bman.local_tasks import generate_custom_settings, generate_site_config
from bman.logger import get_logger


def deploy_ozone(cluster):
    if cluster.is_ozone_enabled():
        generate_ozone_site(cluster)
        make_ozone_directories(cluster)


def make_ozone_directories(cluster):
    # TODO: Make Ozone directories here on all nodes.
    targets = cluster.get_all_hosts()
    if not execute(create_ozone_metadata_paths, hosts=targets, cluster=cluster):
        get_logger().error('Failed to create Ozone directories.')
        return False


@task
def create_ozone_metadata_paths(cluster):
    """"Creates Ozone metadata paths. """
    if cluster.is_ozone_enabled():
        sudo('mkdir -p %s' % cluster.get_config(constants.KEY_OZONE_METADIR),
             user=constants.HDFS_USER)
        sudo('mkdir -p {}' % os.path.dirname(cluster.get_config(constants.KEY_SCM_DATANODE_ID)),
             user=constants.HDFS_USER)
        sudo('mkdir -p %s' % cluster.get_config(constants.KEY_CBLOCK_CACHE_PATH),
             user=constants.HDFS_USER)
        sudo('mkdir -p %s' % cluster.get_config(constants.KEY_OZONE_METADIR),
             user=constants.HDFS_USER)
    return True


def generate_ozone_site(cluster):
    generate_site_config(cluster, filename='ozone-site.xml',
                         settings_key=constants.KEY_OZONE_SITE_SETTINGS,
                         output_dir=cluster.get_generated_hadoop_conf_tmp_dir())


if __name__ == '__main__':
    pass
