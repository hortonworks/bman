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

from setuptools import setup
from setuptools import find_packages

setup(name='bman',
      version='0.4.0-SNAPSHOT',
      description='bman Apache Hadoop Cluster Deployer',
      url='http://github.com/hortonworks/hdfs-tools',
      author='Hortonworks Inc.',
      license='Apache License, Version 2.0',
      packages=find_packages(),   # Recursively discover sub-modules, so resources get picked up.
      zip_safe=False,
      include_package_data=True,  # So resources get installed under site-packages.
      entry_points={
          'console_scripts': [
              'bman = bman.__main__:main'
          ]
      },
      install_requires=[
          "colorama>=0.3.7",
          "daiquiri>=1.3.0",
          "ecdsa==0.13",
          "Fabric3>=1.13.1",
          "paramiko>=1.17.0",
          "pexpect==4.2.1",
          "prompt_toolkit==1.0.9",
          "pycrypto>=2.6.1",
          "pygments>=2.1.3",
          "pytest>=3.0.5",
          "pyyaml>=3.12"
      ])
