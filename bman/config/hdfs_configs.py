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

import re
from urllib import parse as url_parser

from bman.exceptions import ConfigurationError
from bman.logger import get_logger


class HdfsConfigs:
    """
    This class is used to parse and store HDFS configuration for all of the
    following situations:
        - Non-HA config
        - HA config
        - Federation config

    and valid combinations of the above.

    It also parses the other settings in hdfs-site.xml.
    """

    def __init__(self, config_values):
        # Init default config values.
        self.nameservices = []

        # Parse from actual configuration.
        self.parse_nameservices(config_values)

        self.snn_dirs = self.init_snn_dirs(config_values)

    def parse_nameservices(self, values):
        """
        Extract HA config from the given key-value pairs which represent
        hdfs-site.xml settings.
        :param values:
        :return:
        """
        for ns_key in ['dfs.internal.nameservices', 'dfs.nameservices']:
            if ns_key in values:
                for nsid in values[ns_key].split(','):
                    ns = NameService(values, nsid)
                    self.nameservices.append(ns)
                break

        if not self.nameservices:
            get_logger().debug("NameNode HA is not enabled and cluster is not federated.")
            self.nameservices = [NameService(values)]
            return

    def get_nn_hosts(self):
        all_nn_configs = []
        for ns in self.nameservices:
            all_nn_configs += ns.nn_configs
        return list({x.hostname for x in all_nn_configs})

    def get_jn_hosts(self):
        all_jn_hosts = []
        for ns in self.nameservices:
            all_jn_hosts += ns.jn_hosts
        return list(set(all_jn_hosts))

    def get_snn_hosts(self):
        all_snn_hosts = []
        for ns in self.nameservices:
            all_snn_hosts += ns.snn_hosts
        return list(set(all_snn_hosts))

    def choose_active_nns(self):
        # Arbitrarily choose one NN in each namespace as the active.
        # Currently we choose the NN whose hostname is first in lexical order.
        active_nns = []
        for ns in self.nameservices:
            active_nns += ns.choose_active_nn()
        return [x.hostname for x in active_nns]

    def choose_standby_nns(self):
        # Arbitrarily choose one NN in each namespace as the standby.
        # Currently we choose the NN whose hostname is second in lexical order.
        standby_nns = []
        for ns in self.nameservices:
            standby_nns += ns.choose_standby_nns()
        return [x.hostname for x in standby_nns]

    def get_nn_dirs(self):
        dirs = []
        for ns in self.nameservices:
            for nn in ns.nn_configs:
                dirs += nn.dirs
        return dirs

    def get_snn_dirs(self):
        return self.snn_dirs

    def get_jn_dirs(self):
        jn_dirs = []
        for ns in self.nameservices:
            jn_dirs += ns.jn_edits_dirs
        return list(set(jn_dirs))

    @staticmethod
    def init_snn_dirs(config_values):
        dirs = []
        if 'dfs.namenode.checkpoint.dir' in config_values:
            dirs += config_values['dfs.namenode.checkpoint.dir'].split(',')
        if 'dfs.namenode.checkpoint.edits.dir' in config_values:
            dirs += config_values['dfs.namenode.checkpoint.edits.dir'].split(',')
        return dirs

    def get_nameservices(self):
        return self.nameservices

    def __str__(self):
        return str([str(n) for n in self.nameservices])


class NameNodeInfo:
    """
    This class stores the configuration for one specific NameNode.
    TODO: Support per-nn configuration of metadata directories.
    """

    def __init__(self, ns_id, nn_id, hostname, values):
        """
        Parse configuration for one NameNode from hdfs-site.xml settings.
        """
        self.is_pseudo = not bool(nn_id)
        self.ns_id = ns_id
        self.nn_id = nn_id if nn_id else 'pseudo'
        self.hostname = hostname
        self.dirs = []
        self.dirs = self.parse_nn_dirs(values)

    def __lt__(self, other):
        return self.hostname < other.hostname

    def __str__(self):
        return self.hostname

    def parse_nn_dirs(self, values):
        dirs = []
        for conf_key in ['dfs.namenode.name.dir.{}.{}'.format(self.ns_id, self.nn_id),
                         'dfs.namenode.name.dir']:
            if conf_key in values:
                dirs += values[conf_key].split(',')
                break
        for conf_key in ['dfs.namenode.edits.dir.{}.{}'.format(self.ns_id, self.nn_id),
                         'dfs.namenode.edits.dir']:
            if conf_key in values:
                dirs += values[conf_key].split(',')
                break
        return dirs

    def get_id(self):
        return self.nn_id

    def get_hostname(self):
        return self.hostname

    def write_configuration(self, writer):
        """
        Write configuration using the given method.
        :param writer: A method which is invoked with config as strings.
        :return:
        """
        if not self.is_pseudo:
            writer("NamenodeID           : {}".format(self.nn_id))
        writer("NameNode host        : {}".format(self.hostname))
        writer("NameNode dirs        : {}".format(self.dirs))


class NameService(object):
    """
    A class that encapsulates master node information for a single HDFS nameservice.
    """
    def __init__(self, values, nsid=None):
        self.is_pseudo = not bool(nsid)
        self.nsid = nsid if nsid else 'pseudo'  # Our nameserviceId
        self.nn_configs = []
        self.jn_hosts = []
        self.snn_hosts = []  # Secondary NameNodes are valid for non-HA nameservices only.
        self.jn_edits_dirs = []
        self.need_snn_config = False
        self.snn_host_key = None
        if nsid:
            self.parse(values)
        else:
            # This is a pseudo nameservice.
            self.init_pseudo_nameservice(values)

    def __str__(self):
        components = []
        if not self.is_pseudo:
            components.append("nsId: {}".format(self.nsid))
        components.append("NameNodeConfigs: {}".format(
            [str(nn_config) for nn_config in self.nn_configs]))
        if self.snn_hosts:
            components.append("SecondaryNameNodeHosts: {}".format(self.snn_hosts))
        if self.jn_hosts:
            components.append("JournalNodeHosts: {}".format(self.jn_hosts))
            components.append("JournalStorageDirectories: {}".format(self.jn_edits_dirs))
        return ", ".join(components)

    def parse(self, values):
        """
        Parse the list of namenodes for this nameservice and construct a NameNodeInfo
        object for each.
        :param values:
        :return:
        """
        self.parse_namenodes(values)
        self.parse_journalnodes(values)

        # Federation without HA.
        if len(self.nn_configs) == 1:
            self.init_snn_nodes(values)

    @staticmethod
    def get_jn_hosts_from_key(value):
        pattern = re.compile('^qjournal://([^/]+)/.+$', re.IGNORECASE)
        hosts = set()
        for host in pattern.match(value).group(1).split(';'):
            hosts.add(host.split(':')[0])
        return list(hosts)

    def init_snn_nodes(self, values):
        if self.nsid.lower() == 'pseudo':
            snn_host_key = 'dfs.namenode.secondary.http-address'
        else:
            snn_host_key = 'dfs.namenode.secondary.http-address.{}'.format(self.nsid)
        if snn_host_key in values:
            self.snn_hosts = [values[snn_host_key]]
        else:
            # Else collocate with the primary NN
            # TODO: it will be better to pick a non-NN host here but that is complex.
            self.snn_hosts = [self.nn_configs[0].hostname]
            get_logger().debug("For NS {}, placing SNN on host {}".format(
                self.nsid, self.snn_hosts[0]))
            self.need_snn_config = True
            self.snn_host_key = snn_host_key

    def init_pseudo_nameservice(self, values):
        """
        Pseudo-nameservice for non-HA and non-federated cluster.
        """
        fs_url = url_parser.urlparse(values['fs.defaultFS'])
        if fs_url.scheme and fs_url.scheme in {'hdfs', 'viewfs'}:
            self.nn_configs.append(NameNodeInfo(
                self.nsid, None, fs_url.netloc.split(":")[0], values))
        else:
            raise ConfigurationError(
                "Bad fs.defaultFS '{}'. The scheme must be specified as " +
                "'hdfs' or 'viewfs'.\n".format(values['fs.defaultFS']))

    def is_ha(self):
        return len(self.nn_configs) > 1

    def get_id(self):
        return self.nsid

    def choose_active_nn(self):
        """Return a list of active NameNodes."""
        return sorted(self.nn_configs)[0:1]

    def choose_standby_nns(self):
        """Return a (possibly empty) list of standby NameNodes."""
        return sorted(self.nn_configs)[1:2]

    def get_nn_configs(self):
        return self.nn_configs

    def get_uri(self):
        if self.is_ha():
            return 'hdfs://{}'.format(self.nsid)
        else:
            return 'hdfs://{}'.format(self.nn_configs[0].get_hostname())

    def parse_namenodes(self, values):
        ha_nn_key = 'dfs.ha.namenodes.{}'.format(self.nsid)
        if ha_nn_key not in values:
            # NameNode list not defined. The nameservice is not enabled for HA.
            rpc_address_key = 'dfs.namenode.rpc-address.{}'.format(self.nsid)
            if rpc_address_key in values:
                hostname = values[rpc_address_key].split(':')[0]
                self.nn_configs.append(NameNodeInfo(self.nsid, None, hostname, values))
                return
            else:
                raise ConfigurationError('Unable to find NameNode host for nameservice {}'.format(
                    self.nsid))

        for nn_id in values[ha_nn_key].split(','):
            rpc_address_key = 'dfs.namenode.rpc-address.{}.{}'.format(self.nsid, nn_id)
            if rpc_address_key in values:
                hostname = values[rpc_address_key].split(':')[0]
                self.nn_configs.append(NameNodeInfo(self.nsid, nn_id, hostname, values))
            else:
                raise ConfigurationError('Unable to find NameNode host for nameservice {}, nn {}'.format(
                    self.nsid, nn_id))

    def parse_journalnodes(self, values):
        jn_key = 'dfs.namenode.shared.edits.dir.{}'.format(self.nsid)
        if jn_key not in values:
            # Using global config for JN hosts.
            jn_key = 'dfs.namenode.shared.edits.dir'
        if jn_key in values:
            self.jn_hosts = self.get_jn_hosts_from_key(values[jn_key])

        # Initialize the JournalNode edits directories.
        if self.jn_hosts:
            for jn_edits_dir_key in ['dfs.journalnode.edits.dir.{}'.format(self.nsid),
                                     'dfs.journalnode.edits.dir']:
                if jn_edits_dir_key in values:
                    self.jn_edits_dirs = values[jn_edits_dir_key].split(',')
                    break

        if len(self.nn_configs) > 1 and len(self.jn_hosts) == 0:
            raise ConfigurationError("JournalNodes not defined for HA nameservice {}".format(self.nsid))
        elif len(self.nn_configs) == 1 and len(self.jn_hosts) > 0:
            raise ConfigurationError("JournalNodes must not be specified in non-HA mode")


if __name__ == '__main__':
    pass
