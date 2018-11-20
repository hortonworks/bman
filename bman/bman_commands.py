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

from __future__ import print_function
from __future__ import print_function

import fabric
from colorama import Fore
from fabric.api import env, local, hide
from prompt_toolkit import prompt

import bman.config.bman_config as config
import bman.constants as constants
from bman.logger import get_logger
from bman.remote_tasks import prepare_cluster, run_hdfs, run_yarn, run_ozone, start_stop_datanodes, \
    start_stop_namenodes, start_stop_journalnodes, shutdown, add_user
from bman.service_installers.common import install_cluster
from bman.utils import is_true, do_sleep


class BmanCommandHandler:

    def __init__(self, cluster):
        self.handlers = {
            'config': self.handle_show,
            'help': self.handle_help,
            'quit': self.handle_quit,
            'namenodes': self.handle_namenode,
            'datanodes': self.handle_datanode,
            'journalnodes': self.handle_journalnode,
            'wipe': self.handle_wipe_cluster,
            'prepare': self.handle_prepare_cluster,
            'install': self.handle_install,
            'deploy': self.handle_deploy,
            'run': self.handle_start,
            'start': self.handle_start,
            'stop': self.handle_stop,
            'shutdown': self.handle_shutdown,
            'useradd': self.handle_useradd,
            'dump_config': self.handle_dump_config
        }
        self.init_fabric_env_auth_settings(cluster)

    @staticmethod
    def handle_help(command, cluster):
        print("\nCommands supported are:")
        print(Fore.CYAN + "\thelp" + Fore.RESET + "\t\t - print this help message.")
        print(Fore.CYAN + "\tquit" + Fore.RESET + "\t\t - quit this shell.")
        print(Fore.CYAN + "\tconfig" + Fore.RESET + "\t\t - print the current cluster configration.")
        print(Fore.CYAN + "\tnamenodes" + Fore.RESET + "\t - print the list of namenodes.")
        print(Fore.CYAN + "\tdatanodes" + Fore.RESET + "\t - print the list of datanodes.")
        print(Fore.CYAN + "\twipe" + Fore.RESET + "\t\t - wipe all nodes in the cluster")
        print(Fore.CYAN + "\tinstall" + Fore.RESET + "\t\t - install the cluster and start all services")
        print(Fore.CYAN + "\tstart [dfs|yarn|ozone|namenodes|datanodes]" + Fore.RESET + "\t\t - start all or some services")
        print(Fore.CYAN + "\tstop [dfs|yarn|ozone|namenodes|datanodes]" + Fore.RESET + "\t\t - stop all or some services")
        print(Fore.CYAN + "\tshutdown" + Fore.RESET + "\t - stop all running services.\n")
        print(Fore.CYAN + "\tuseradd <user> <password>" + Fore.RESET + "\t - create a new user on all nodes.\n")
        print(Fore.CYAN + "\tuserdel <user>" + Fore.RESET + "\t - Delete the user on all nodes.\n")
        print(Fore.CYAN + "\tdump_config" + Fore.RESET + "\t - Print configuration summary (for debugging).\n")
        print("To install the cluster, please edit the file " + Fore.CYAN +
              cluster.get_config_file() + Fore.RESET + " and set appropriate values.")

    @staticmethod
    def handle_show(command, cluster):
        if command.lower() == 'config':
            print(cluster)

    @staticmethod
    def handle_quit(command, cluster):
        print("Goodbye")
        exit(0)

    @staticmethod
    def handle_namenode(command, cluster):
        print("NameNodes: {}".format(cluster.get_hdfs_configs().get_nn_hosts()))

    @staticmethod
    def handle_datanode(command, cluster):
        print("DataNodes : {}".format(cluster.get_worker_nodes()))

    @staticmethod
    def handle_journalnode(command, cluster):
        print("JournalNodes : {}".format(cluster.get_hdfs_configs().get_jn_hosts()))

    @staticmethod
    def handle_wipe_cluster(command, cluster):
        env.output_prefix = False
        # Convert from string to binary
        prepare_cluster(cluster=cluster, force=is_true(cluster.config[constants.KEY_FORCE_WIPE]))
        get_logger().info("Done wiping the cluster.")

    @staticmethod
    def handle_prepare_cluster(command, cluster):
        get_logger().warn("prepare command is deprecated. Use 'wipe' instead.")
        BmanCommandHandler.handle_wipe_cluster(command, cluster)

    @staticmethod
    def handle_install(command, cluster, stop_services=True):
        install_cluster(cluster=cluster, stop_services=stop_services)
        get_logger().debug("Finished installing.")

    @staticmethod
    def handle_deploy(command, cluster):
        BmanCommandHandler.handle_install(command, cluster, stop_services=False)

    @staticmethod
    def handle_start(command, cluster):
        service = command.split()[1:2]
        nodes = command.split()[2:]
        cmds = []
        if not service or service[0].lower() == "all":
            run_hdfs(cluster)
            do_sleep(20)
            if cluster.is_yarn_enabled():
                run_yarn(cluster)
        elif service[0].lower() in {'dfs', 'hdfs'}:
            run_hdfs(cluster)
        elif service[0].lower() == 'yarn':
            run_yarn(cluster)
        elif service[0].lower() == 'ozone':
            run_ozone()
        elif service[0].lower() == 'datanodes':
            start_stop_datanodes(action='start', nodes=nodes, cluster=cluster)
        elif service[0].lower() == 'namenodes':
            start_stop_namenodes(action='start', nodes=nodes, cluster=cluster)
        elif service[0].lower() == 'journalnodes':
            start_stop_journalnodes(action='start', nodes=nodes, cluster=cluster)
        else:
            get_logger().error("Unrecognized service {}\n".format(service[0]))
            return
        with hide('running'):
            for cmd in cmds:
                local(cmd)

    @staticmethod
    def handle_stop(command, cluster):
        service = command.split()[1:2]
        nodes = command.split()[2:]
        cmds = []
        if not service or service[0].lower() == "all":
            shutdown(cluster)
        elif service[0].lower() in {'dfs', 'hdfs', 'ozone'}:
            shutdown(cluster)
        elif service[0].lower() == 'datanodes':
            start_stop_datanodes(action='stop', nodes=nodes, cluster=cluster)
        elif service[0].lower() == 'namenodes':
            start_stop_namenodes(action='stop', nodes=nodes, cluster=cluster)
        elif service[0].lower() == 'journalnodes':
            start_stop_journalnodes(action='stop', nodes=nodes, cluster=cluster)
        else:
            get_logger().error("Unrecognized service {}\n".format(service[0]))
            return
        with hide('running'):
            for cmd in cmds:
                local(cmd)

    @staticmethod
    def handle_shutdown(command, cluster):
        try:
            shutdown(cluster)
        except Exception as e:
            get_logger().error(e)
            raise

    @staticmethod
    def handle_useradd(command, cluster):
        if len(command.split()) != 4:
            print("Usage: useradd <username> <password> <groupname>")
            print("    The group will be created if it doesn't exist.")
            return
        add_user(cluster, config.UserConfig(*command.split()[1:]))

    @staticmethod
    def handle_dump_config(command, cluster):
        print("Configuration Summary: {}".format(cluster))

    def handle_command(self, command, cluster):
        if not command:
            return True

        commands = command.lower().split()
        if commands[0] not in self.handlers:
            get_logger().error("Unknown command: {}\n".format(command))
            return False

        try:
            self.handlers[commands[0]](command, cluster)
        except Exception as e:
            get_logger().error(e)
            raise
        finally:
            # Close connections so the shell does not hang
            # on exit. See fabric documentation for more.
            fabric.network.disconnect_all()

    @staticmethod
    def init_fabric_env_auth_settings(cluster):
        # Make sure we have no implicit dependency on keys in the user's
        # .ssh/ directory. Credentials must be specified explicitly in the
        # YAML file.
        env.no_keys = True

        env.user = cluster.config[constants.KEY_USER]
        if cluster.config[constants.KEY_SSH_KEYFILE]:
            env.key_filename = cluster.config[constants.KEY_SSH_KEYFILE]
        else:
            if not cluster.config[constants.KEY_PASSWORD]:
                msg = u"Please enter {}'s password for the remote cluster: ".format(
                    cluster.config[constants.KEY_USER])
                cluster.config[constants.KEY_PASSWORD] = prompt(msg, is_password=True)
                env.password = cluster.config[constants.KEY_PASSWORD]


if __name__ == '__main__':
    pass
