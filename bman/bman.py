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

from __future__ import unicode_literals

import os.path

import sys
from prompt_toolkit import prompt
from prompt_toolkit.contrib.completers import WordCompleter
from prompt_toolkit.history import FileHistory
from pygments.style import Style
from pygments.styles.default import DefaultStyle
from pygments.token import Token

import bman.bman_commands as bman_commands
from bman import constants
from bman.logger import get_logger
from bman.config.bman_config import load_config

commands_list = ['config', 'help', 'quit', 'namenodes', 'datanodes',
                 'journalnodes', 'cluster', 'tarball', 'prepare',
                 'deploy', 'hdfs', 'cblock', 'start', 'stop',
                 'mapred', 'yarn', 'nodemanager', 'resourcemanager',
                 'useradd']
command_completer = WordCompleter(commands_list, ignore_case=True)


class DocumentStyle(Style):
    styles = {
        Token.Menu.Completions.Completion.Current: 'bg:#00aaaa #000000',
        Token.Menu.Completions.Completion: 'bg:#008888 #ffffff',
        Token.Menu.Completions.ProgressButton: 'bg:#003333',
        Token.Menu.Completions.ProgressBar: 'bg:#00aaaa',
    }
    styles.update(DefaultStyle.styles)


def welcome():
    print("Press ctrl-D or type 'quit' to exit.")
    print("Type 'help' to get the list of commands.")


def fail_if_fabricrc_exists():
    """
    Fail if ~/.fabricrc exists. Credentials defined in .fabricrc can
    conflict with credentials generated by bman.
    :return:
    """
    fabricconf = os.path.join(os.path.expanduser("~"), ".fabricrc")
    if os.path.isfile(fabricconf):
        get_logger().error("Please remove the {} file.".format(fabricconf))
        sys.exit(-1)


def get_shell_history():
    """
    Setup persistent history in ~/.config/bman/history.bman.
    :return:
    """
    history_dir = os.path.join(os.path.expanduser("~"), ".config", "bman")
    os.makedirs(history_dir, exist_ok=True)
    history_file = os.path.join(history_dir, "history.bman")
    return FileHistory(history_file)


def run_bman(argv):
    """ Main command loop for shell. """
    fail_if_fabricrc_exists()
    cluster = load_config()
    if len(argv) == 1:
        # No arguments provided. Launch the shell.
        launch_interactive_shell(cluster)
    else:
        # Interpret the arguments as a command and execute them.
        bman_commands.BmanCommandHandler(cluster).handle_command(
            ' '.join(argv[1:]), cluster)


def launch_interactive_shell(cluster):
    history = get_shell_history()
    welcome()
    bman_command_handler = bman_commands.BmanCommandHandler(cluster)
    while True:
        try:
            text = prompt(message='{}> '.format(cluster.get_config(constants.KEY_CLUSTER_NAME)),
                          completer=command_completer,
                          style=DocumentStyle,
                          history=history)
            bman_command_handler.handle_command(text, cluster)
        except EOFError:
            break
        except KeyboardInterrupt:
            break
    print('Goodbye')


if __name__ == '__main__':
    run_bman(sys.argv)
