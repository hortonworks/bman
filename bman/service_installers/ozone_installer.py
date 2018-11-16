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
from bman.local_tasks import generate_custom_settings
from bman.logger import get_logger


def deploy_ozone(cluster):
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        generate_ozone_site(cluster)


def make_ozone_directories(cluster, targets):
    # TODO: Make Ozone directories here on all nodes.
    if cluster.is_ozone_enabled():
        sudo('install -d -m 0755 {}'.format(cluster.get_config(constants.KEY_OZONE_METADIR)))
        sudo('chown -R hdfs:hadoop {}'.format(cluster.get_config(constants.KEY_OZONE_METADIR)))

    if not execute(create_ozone_metadata_paths, hosts=targets, cluster=cluster):
        get_logger().error('Create directories failed.')
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
    template_str = resource_string('bman.resources.conf', 'ozone-site.xml.template').decode('utf-8')
    site_string = string.Template(template_str)
    ozone_str = site_string.substitute(
        OzoneEnabled=cluster.get_config(constants.KEY_OZONE_ENABLED),
        OzoneMetadataDir=cluster.get_config(constants.KEY_OZONE_METADIR),
        ScmServerAddress=cluster.get_config(constants.KEY_SCMADDRESS),
        OzoneDatanodeID=cluster.get_config(constants.KEY_SCM_DATANODE_ID),
        cBlockServerAddress=cluster.get_config(constants.KEY_CBLOCK_ADDRESS),
        cBlockCacheEnabled=cluster.get_config(constants.KEY_CBLOCK_CACHE),
        cBlockTraceEnabled=cluster.get_config(constants.KEY_CBLOCK_TRACE),
        CBlockCachePath=cluster.get_config(constants.KEY_CBLOCK_CACHE_PATH),
        OzoneCustomConfig=generate_custom_settings(
            cluster.get_config(constants.KEY_OZONE_SITE_SETTINGS)))

    with open(os.path.join(
            cluster.get_generated_hadoop_conf_tmp_dir(), "ozone-site.xml"), "w") as ozone_site:
        ozone_site.write(ozone_str)


if __name__ == '__main__':
    pass
