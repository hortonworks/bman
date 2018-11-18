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

from bman.logger import get_logger

"""
A few utility functions to read and save configuration values.
"""


def read_config_value_with_default(config_map, values, key, default_value=None):
    """
    Read a config key from values and update config_map.
    If absent then use the default.
    :param config_map: Map object to be updated.
    :param values:
    :param key:
    :param default_value:
    :return:
    """
    if key in values and values[key]:
        config_map[key] = values[key]
    else:
        config_map[key] = default_value


def read_config_value_with_altkey(config_map, values, key, altkey):
    """
    Read a config key from values and update config_map.
    If absent then look for the altkey.
    :param config_map: Map object to be updated.
    :param values:
    :param key:
    :param altkey:
    :return:
    """
    value = None
    if key in values and values[key]:
        config_map[key] = value = values[key]
    if not value and altkey in values and values[altkey]:
        get_logger().warn("{} has been deprecated by {}".format(
            altkey, key))
        config_map[key] = values[altkey]
    if not value:
        raise ValueError("Required key {} is missing in YAML.".format(key))


def read_required_config_value(config_map, values, key):
    value = None
    if key in values:
        config_map[key] = value = values[key]
    if not value:
        raise ValueError("Required key : {} is missing.".format(key))