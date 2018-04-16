## Bman Cluster Manager

The Bman cluster manager is named after the legendary indian warrior named Bheem. He was a warrior with the strength of 10 thousand elephants, and we hope this cluster manager is able to manage thousands of machines.

Bman is a python tool that deploys Apache Hadoop tarballs to a cluster. Bman reads a set of configuration values from an YAML file called `config.yaml`. This configuration file describes the machines in the cluster as well as Hadoop settings.

### Prerequisites

Bman requires
1. Python 3.5 or later on bman host (Python is not required on the cluster nodes).
1. Valid configuration file `~/.config/bman/config.yaml` on bman host (more on this below).
1. Java 1.8 on cluster nodes.
1. If enabling Kerberos, then:
    1. 1. Cluster hosts running Linux. The `bman` developers test with Centos 7 however most Linux distributions should work well.
    1. kadmin hostname and credentials.
    1. KDC and kadmin servers reachable from all cluster nodes.
    1. Kerberos client packages on all cluster nodes. e.g. The requisite packages can be installed on Centos with `yum install -y krb5-libs rng-tools krb5-workstation`. bman will not install Kerberos packages.
    1. Java Cryptography Extension (JCE) Unlimited Strength policy files on bman host.

## Using bman

### Install Python package

bman can be installed as a Python3 package. Download a [release package from GitHub](https://github.com/hortonworks/bman/releases) and install it with pip e.g.

```
$ pip3 install bman-0.1.tar.gz
```
_bman is not available on pypi yet._


### Create config.yaml

`bman` is driven by a YAML file called `config.yaml`. It is intended to be a self-documenting configuration file.

Copy the supplied `config.yaml.template` to `~/.config/bman/config.yaml`. Edit `config.yaml` as appropriate for your cluster by defining the cluster nodes, location of the Hadoop distribution tarball, locations of NameNode metadata and DataNode storage directories and any custom site settings for `core-site.xml`, `hdfs-site.xml` etc. (optional).

### Non-interactive use

`bman` is scriptable e.g. the following shell script installs Apache Hadoop on a cluster. In scriptable mode, the `ForceWipe` property must be set to `True` in `config.yaml`.

```
#!/usr/bin/env bash

set -euo pipefail

bman prepare       # Wipe existing data on cluster nodes.
bman deploy        # Deploy packages and config files, and start all services.
...
```

As the script shows, cluster installation occurs in two steps:
1. `prepare`: The existing cluster data is wiped. Service users are recreated.
1. `deploy`: Hadoop config files are generated. The Hadoop distribution and config files are copied to all cluster nodes. If Kerberos is enabled, then service principals and keytabs are created. Also the HDFS NameNode is formatted at this step, 'tmp' directories created and (optionally) Tez distribution is uploaded to the cluster. Finally services are started.


### Interactive shell
Run `bman` without any parameters to launch the interactive shell.
```
$ bman
Press ctrl-D or type 'quit' to exit.
Type 'help' to get the list of commands.
HdfsDev> prepare
...
HdfsDev> deploy
...
HdfsDev> stop all

```

### Enabling Hadoop Security via Kerberos

`bman` can enable Hadoop Security on the cluster. This requires the following settings in `config.yaml` (see the template for more details):

1. KadminServer
1. KadminPrincipal
1. KadminPassword
1. KerberosRealm

Additionally, the following four settings must be defined in `CoreSiteSettings`:
```
CoreSiteSettings:
...
  hadoop.security.authentication: 'kerberos'
  hadoop.security.authorization: 'true'
  hadoop.rpc.protection: 'authentication'
  hadoop.security.auth_to_local: |-
    RULE:[2:$1@$0](rm@.*REALM)s/.*/yarn/
    RULE:[2:$1@$0](nm@.*REALM)s/.*/yarn/
    RULE:[2:$1@$0](nn@.*REALM)s/.*/hdfs/
    RULE:[2:$1@$0](dn@.*REALM)s/.*/hdfs/
    RULE:[2:$1@$0](snn@.*REALM)s/.*/hdfs/
    RULE:[2:$1@$0](jn@.*REALM)s/.*/hdfs/
    RULE:[2:$1@$0](jhs@.*REALM)s/.*/mapred/
    DEFAULT
```

Hadoop security requires many other configuration settings including principals, service keytabs and other NameNode/DataNode settings. `bman` will auto-generate sensible values for all of these.

It is assumed that you have installed Kerberos client on all the cluster nodes and that all nodes have a valid `/etc/krb5.conf` file.

## config.yaml

Here is a set of required values in the `config.yaml`.

1. _Cluster_ : The name of the cluster, this string is used as the prompt for the bman shell.
1. _Workers_ : The machines were DataNode and NodeManager services will run.
1. _HadoopTarball_ : Hadoop tarball which we want to deploy to the cluster.
1. _TezTarball_ : (Optional) Tez tarball which we want to deploy to the cluster. The Tez build must be compatible with the Hadoop build. See [Compiling Tez with Apache Hadoop 2.8.0 or later](https://community.hortonworks.com/articles/175871/compiling-apache-tez-with-apache-hadoop-280-or-lat.html) for instructions on building Tez.
1. _HomeDir_ : Location where services will be deployed to.
1. _CoreSiteSettings_: Dictionary of configuration settings to generate `core-site.xml`. The only required setting is `fs.defaultFS`.
1. _HdfsSiteSettings_: Dictionary of configuration settings to generate `hdfs-site.xml`.
1. _YarnSiteSettings_: (Optional) Dictionary of configuration settings to generate `yarn-site.xml`. If absent, then YARN services will not be started. There is only one mandatory yarn-site.xml setting: `yarn.resourcemanager.address`.
1. _MapredSiteSettings_: (Optional) Dictionary of configuration settings generate `mapred-site.xml`.
1. _TezSiteSettings_: (Optional) Dictionary of configuration settings generate `tez-site.xml`. 
1. _OzoneSiteSettings_: (Optional) Dictionary of configuration settings to generate `ozone-site.xml`.

There are a bunch of settings like `OzoneEnabled` or `CblockCacheEnabled`, which can be turned on by the user if they want to run Ozone or cblocks. Once again please take a look at `config.yaml`.


## Developer Documentation

### Install from Source code

Clone the source repository. Install venv (virtual env) and dependencies from a POSIX compatible shell like bash (zsh should also work). venv does not support alternative shells like fish.

```
$ pip3 install virtualenv
$ virtualenv -p $(which python3) venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ brew install https://raw.githubusercontent.com/kadwanev/bigboybrew/master/Library/Formula/sshpass.rb 
```

Now you should have a working virtual env.
```
(venv) username@hostname /bman$ fab --list
```

#### Activate venv
Before using bman, activate the Python venv with:
```
source venv/bin/activate
```

#### Launch the shell
In developer mode, start the bman shell with ```python -m bman```.
```
(venv) username@hostname /bman$ python -m bman
Press ctrl-D or type 'quit' to exit.
Type 'help' to get the list of commands.
HDFS Dev >
```

### Source code overview

`bman.py` - Trivial wrapper module to launch the shell.

`bman/bman.py` - is a simple shell loop, it reads commands from the user and dispatches them to commands.py.

`bman/bman_commands.py` - is the command parser that dispatches to execution methods.

`bman/remote_tasks.py` - Most commands that work against the cluster is located in this file.

`bman/deployment_manager.py` - understands the steps necessary to deploy different Apache Hadoop configurations (with/without NameNode HA, federation, and combinations thereof).

`bman/local_tasks.py` - contains routines to generate the config, private key, routines to copy the files to right location etc.

`bman/bman_config.py` - reads the YAML file and puts each of the key into map. These keys can be  accessed via calling to `cluster.get_config`. To add new keys, please define a Key name at the top of the file and use appropriate key reading function in the `cluster_constructor` function.

`utils.py` - A few utility functions used by both `local_tasks.py` and `remote_tasks.py`.

These keys can be accessed anywhere using the `cluster.get_config`. You can see many examples in the code.

### Building a Python Package

If you make source code changes and wish to build a new Python package for testing/release, run the following commands. If you are building a new release, you must update the package version in `setup.py` before building the package.


```
$ rm -fr bman.egg-info/ dist/
$ python setup.py sdist
$ pip3 uninstall -y bman
$ pip3 install $PWD/dist/bman-*.tar.gz
```

The first step is important to ensure your changes are picked up. See [Python Packaging Pitfalls](https://blog.ionelmc.ro/2014/06/25/python-packaging-pitfalls/)

## Credits

bman includes contributions from @anuengineer, @arp7, @elek, @mukul1987, @nandakumar131, @chen-liang and @ajayydv.

*Apache®, Apache Hadoop, Hadoop®, and the yellow elephant logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.*
