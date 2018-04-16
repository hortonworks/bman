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

# This file contains local tasks used command shell and the script.

import daiquiri
import logging

import os
import sys

COMPLEX_FORMATTER="%(asctime)s - %(name)s - %(levelname)s - %(module)s : " + \
                        "%(lineno)d - %(message)s"

# Singleton logger object for all bman modules.
logger = None


def get_logger():
    """
    Return the logger object.
    TODO: Initialization of the logger is not thread-safe. This is okay for now
    as bman is single-threaded.
    """
    global logger
    if not logger:
        """
        Initialize the logger. Write log output to a file under logs/ and to STDERR.
        """
        log_dir = os.path.join(os.path.expanduser('~'), '.bman-logs')
        os.makedirs(log_dir, exist_ok=True)
        daiquiri.setup(
            level=logging.DEBUG,
            outputs=(
                daiquiri.output.TimedRotatingFile(
                    os.path.join(log_dir, 'bman.log'),
                    level=logging.DEBUG,
                    formatter=logging.Formatter(fmt=COMPLEX_FORMATTER)),
                daiquiri.output.Stream(stream=sys.stderr, level=logging.INFO)
            )
        )
        logger = daiquiri.getLogger("bman.shell")
        logging.getLogger("paramiko").setLevel(logging.WARNING)  # paramiko logs excessively at INFO.
    return logger


if __name__ == '__main__':
    pass
