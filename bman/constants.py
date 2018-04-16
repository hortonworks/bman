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
String and numeric constants used in bman.
"""

KEY_NAME = 'Cluster'
KEY_USER = 'User'
KEY_PASSWORD = 'Password'
KEY_SSH_KEYFILE = 'SshKeyFile'
KEY_WORKERS = 'Workers'
KEY_HADOOP_TARBALL = 'HadoopTarball'
KEY_TEZ_TARBALL = 'TezTarball'
KEY_HOMEDIR = 'HomeDir'
KEY_SCMADDRESS = 'ScmServerAddress'
KEY_OZONE_METADIR = 'OzoneMetadataDir'
KEY_SCM_DATANODE_ID = 'SCMDatanodeID'
KEY_CBLOCK_ADDRESS = 'cBlockServerAddress'
KEY_CBLOCK_CACHE = 'CBlockCacheEnabled'
KEY_CBLOCK_TRACE = 'CBlockCacheTraceEnabled'
KEY_OZONE_ENABLED = 'OzoneEnabled'
KEY_CBLOCK_CACHE_PATH = 'CBlockCachePath'
KEY_CORE_SITE_SETTINGS = 'CoreSiteSettings'
KEY_HDFS_SITE_SETTINGS = 'HdfsSiteSettings'
KEY_YARN_SITE_SETTINGS = 'YarnSiteSettings'
KEY_MAPRED_SITE_SETTINGS = 'MapredSiteSettings'
KEY_OZONE_SITE_SETTINGS = 'OzoneSiteSettings'
KEY_TEZ_SITE_SETTINGS = 'TezSiteSettings'
KEY_FORCE_WIPE = 'ForceWipe'
KEY_JCE_POLICY_FILES_LOCATION = 'JcePolicyFilesLocation'
KEY_REALM = 'KerberosRealm'

KEY_JAVA_HOME = 'JavaHome'
DEFAULT_JAVA_HOME = '/usr/java/latest'
JSVC_HOME = '/usr/lib/jsvc'

HDFS_USER = 'hdfs'
YARN_USER = 'yarn'
MAPREDUCE_USER = 'mapred'
TEZ_USER = 'tez'
HADOOP_GROUP = 'hadoop'
HADOOP_LOG_DIR_NAME = 'logs'  # Directory name under HADOOP_HOME where service logs will be stored.

DEFAULT_NAMENODE_HTTP_PORT = 9870
DEFAULT_SECONDARY_NAMENODE_HTTP_PORT = 9869

DEFAULT_SSH_KEY_NAME = 'id_rsa'  # The default key name that sshd understands

# Deprecated keys. We still parse them for compatibility with older config files.
KEY_DATANODES = 'Datanodes'     # Deprecated by KEY_WORKERS
KEY_TARBALL = 'Tarball'         # Deprecated by KEY_HADOOP_TARBALL

if __name__ == '__main__':
    pass
