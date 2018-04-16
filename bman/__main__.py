# Copyright 2018 Hortonworks Inc.
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

import bman.bman
import sys

"""
Console entry point for the bman package.
"""

MIN_PYTHON_VERSION = (3, 5)


def main():
    """ Launch the shell. """
    if sys.version_info < MIN_PYTHON_VERSION:
        sys.stderr.write("Python {}.{} or later is required.\n".format(*MIN_PYTHON_VERSION))
        sys.exit(-1)
    bman.bman.run_bman(sys.argv)


if __name__ == '__main__':
    main()
