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

# This file contains tasks used command shell and the script.

import io
import os

import fabric
from fabric.api import task, settings, sudo, hide, env, execute
from fabric.decorators import parallel

import bman.constants as constants
import bman.config.bman_config as config
from bman.kerberos_setup import make_headless_principal
from bman.logger import get_logger
from bman.utils import start_stop_service, prompt_for_yes_no, run_dfs_command


def prepare_cluster(cluster=None, force=False):
    """
    Prepares a cluster.
    """
    fabric.state.output.status = False
    env.output_prefix = False
    targets = cluster.get_all_hosts()
    get_logger().info("Preparing {} nodes {}".format(len(targets), targets))
    with hide('status', 'warnings', 'running', 'stdout', 'stderr', 'user', 'commands'):
        if not execute(killall_services, hosts=targets):
            get_logger().error('killing all services failed.')
            return False

        if not execute(clean_tmp, hosts=targets):
            get_logger().error('cleaning tmp failed.')
            return False

        if not execute(clean_root_dir, hosts=targets, path=cluster.get_config(constants.KEY_INSTALL_DIR)):
            get_logger().error("Unable to remove files from the root dir : %s" %
                               cluster.get_config(constants.KEY_INSTALL_DIR))
            return False

        if not execute(make_shell_profile, hosts=targets, install_dir=cluster.get_hadoop_install_dir()):
            get_logger().error('Failed to initialize Hadoop shell profile.')
            return False

        # TODO: wipe the Tez install directory.

        if not execute(delete_service_users, hosts=targets, cluster=cluster):
            get_logger().info("Deleting users failed.")

        for new_user in cluster.get_service_users():
            add_user(cluster, new_user)

        if not execute(wipe_cluster, hosts=targets, cluster=cluster, force=force):
            get_logger().error('Wipe cluster failed.')
            return False
    return True


def do_active_transitions(cluster):
    for ns in cluster.get_hdfs_configs().get_nameservices():
        if len(ns.get_nn_configs()) > 1:
            active_nn_id = ns.choose_active_nn()[0].nn_id
            get_logger().info("Transitioning {}.{} to active".format(ns.nsid, active_nn_id))
            cmd = '{}/bin/hdfs haadmin -ns {} -transitionToActive {}'.format(
                cluster.get_hadoop_install_dir(), ns.nsid, active_nn_id)
            targets = cluster.get_hdfs_configs().get_nn_hosts()[0:1]
            execute(run_dfs_command, hosts=targets, cluster=cluster, cmd=cmd)
    pass


@task
def run_hdfs(cluster):
    targets = cluster.get_hdfs_configs().get_nn_hosts()
    execute(start_dfs, hosts=[targets[0]], cluster=cluster)

    do_active_transitions(cluster)

    # CBlock needs OZONE -- so we start everything.
    if cluster.get_config(constants.KEY_CBLOCK_CACHE):
        # We start SCM and Cblock on the Namenode machine for now.
        # TODO : Fix this so that if other machines are specified
        # we are able to get to it.
        if execute(start_scm, hosts=targets, cluster=cluster):
            get_logger().error('Start SCM failed.')
            return False

        if execute(start_cblock_server, hosts=targets, cluster=cluster):
            get_logger().error('Failed to start cBlock Server.')
            return False

        # Read the Datanode Machine list now and execute the rest of the commands
        targets = cluster.get_worker_nodes()

        if execute(start_jscsi_server, hosts=targets, cluster=cluster):
            get_logger().error('Unable to start datanodes.')
            return False
    else:
        # Just Start SCM for Ozone, everthing else is already running.
        if cluster.get_config(constants.KEY_OZONE_ENABLED):
            if execute(start_scm, hosts=targets, cluster=cluster):
                get_logger().error('Start SCM failed.')
                return False

    return True


@task
def run_yarn(cluster):
    targets = cluster.get_hdfs_configs().get_nn_hosts()
    execute(start_yarn, hosts=[targets[0]], cluster=cluster)


def start_stop_namenodes(action=None, nodes=None, cluster=None):
    targets = nodes if nodes else cluster.get_hdfs_configs().get_nn_hosts()
    execute(start_stop_service, hosts=targets, cluster=cluster,
            action=action, service_name='namenode', user=constants.HDFS_USER)
    return True


def start_stop_datanodes(action=None, nodes=None, cluster=None):
    targets = nodes if nodes else cluster.get_worker_nodes()
    execute(start_stop_service, hosts=targets, cluster=cluster,
            action=action, service_name='datanode', user=constants.HDFS_USER)
    return True


def start_stop_journalnodes(action=None, nodes=None, cluster=None):
    targets = nodes if nodes else cluster.get_hdfs_configs().get_jn_hosts()
    execute(start_stop_service, hosts=targets, cluster=cluster,
            action=action, service_name='journalnode', user=constants.HDFS_USER)
    return True


def run_ozone():
    cluster = config.load_config()
    targets = cluster.get_hdfs_configs().get_nn_hosts()
    execute(start_ozone, hosts=targets[0], cluster=cluster)
    return True


def shutdown(cluster):
    targets = cluster.get_hdfs_configs().get_nn_hosts()

    if cluster.is_yarn_enabled():
        execute(stop_yarn, hosts=targets[0], cluster=cluster)
    execute(stop_dfs, hosts=targets[0], cluster=cluster)

    if not cluster.get_config(constants.KEY_OZONE_ENABLED) and \
            not cluster.get_config(constants.KEY_CBLOCK_CACHE):
        return True

    execute(stop_ozone, hosts=targets[0], cluster=cluster)

    with hide('stdout', 'stderr'):
        execute(kill_cblock_server, hosts=targets)

    targets = cluster.get_worker_nodes()
    with hide('stdout', 'stderr'):
        execute(kill_scsi_server, hosts=targets)
    return True


def create_fabric_rc(fabricconf, home, username, password):
    """
    Creates the RC for fabric access.
    :param fabricconf:
    :param home:
    :param username:
    :param password:
    :return:
    """
    buf = io.StringIO()
    buf.write("user = %s\n" % username)
    buf.write("password = %s\n" % password)
    with open(os.path.join(home, fabricconf), 'w') as fabricrc:
        fabricrc.write(buf.getvalue())
        fabricrc.close()


@task
def killall_services():
    """
    Kills all services related to hadoop.
    :return:
    """
    # Need to add the full list of Hadoop proceess here.
    # with settings(warn_only=True):
    #     if is_namenode_running():
    #         kill_namenode()
    #     if is_datanode_running():
    #         kill_datanode()
    #     if is_scsi_server_running():
    #         kill_scsi_server()
    #     if is_scm_running():
    #         kill_scm()
    #     if is_cblock_running():
    #         kill_cblock_server()
    # return True

    # Cheat for now and kill all Java process
    get_logger().debug("Killing all services on host {}".format(env.host))
    with settings(warn_only=True):
        sudo('ps aux | grep -i [j]ava | awk \'{print $2}\' | xargs -r kill -9')
    get_logger().debug("Killed all services on host {}".format(env.host))
    return True


def clean_tmp():
    get_logger().debug("Cleaning tmp directories on host {}".format(env.host))
    with settings(warn_only=True):
        sudo('rm -rf /tmp/*')
    return True


@task
def clean_root_dir(path):
    """
    Removes all files from the Path.
    :param path:
    :return:
    """
    get_logger().debug("Cleaning root directory on host {}".format(env.host))
    with settings(warn_only=True):
        sudo("rm -rf %s" % path)
    return True


@task
def wipe_cluster(cluster, force=False):
    """ Deletes data on all datanode and namenode disks.

    Options:
        force: If set to True, the user is not prompted. Default: no.
        username: name of the user who is given permissions to
                  the storage directories. Default: hdfs
    """
    try:
        master_config = cluster.get_hdfs_configs()
        if not force:
            force = prompt_for_yes_no(
                "Data will be irrecoverably lost from node {}. "
                "Are you sure ? ".format(env.host))
        if force:
            get_logger().warning('Wiping node {}'.format(env.host_string))
            get_logger().debug('running remove command on {}'.format(env.host_string))
            for d in master_config.get_nn_dirs() + cluster.get_datanode_dirs() + \
                    master_config.get_jn_dirs() + master_config.get_snn_dirs():
                sudo('rm -fr {}/*'.format(d))
            if (cluster.get_config(constants.KEY_OZONE_ENABLED) and
                    os.path.isdir(cluster.get_config(constants.KEY_OZONE_METADIR))):
                sudo('rm -fr {}/*'.format(os.path.isdir(cluster.get_config(constants.KEY_OZONE_METADIR))))
        else:
            get_logger().warning('Skipping machine: %s', env.host_string)
        return True

    except Exception as e:
        get_logger().exception(e)
        return False


@task
def check_user_exists(username=None):
    """
    Checks if the user exists on the remote machine
    """
    get_logger().debug("executing check_user_exists for user {} on host {}".format(
        username, env.host))
    with hide('status', 'aborts', 'warnings', 'running', 'stdout', 'stderr',
              'user', 'commands', 'output'), settings(
        warn_only=True):
        get_logger().debug("user is {} running id {}".format(env.user, username))
        result = sudo('id %s'.format(username), pty=True)
    return result.succeeded


@task
def delete_service_users(cluster=None):
    """ Deletes a user. e.g. fab delete_user:username=hdfs
    Options:
        username: name of the user to delete.
    This command logs into all the machines in the datanodes set and deletes
    a new user.
    """
    for username in [x.name for x in cluster.get_service_users()]:
        get_logger().debug("Deleting user '{}' on host {}".format(username, env.host))
        if execute(check_user_exists, username=username):
            with hide('status', 'aborts', 'warnings', 'running', 'stdout', 'stderr',
                      'user', 'commands', 'output'), settings(warn_only=True):
                result = sudo('userdel -f --remove {}'.format(username))
                if not result.succeeded:
                    get_logger().debug('deleting user {} failed'.format(username))
                    return False
    return True


@task
def make_shell_profile(install_dir):
    """
    Setup a Hadoop shellprofile under /etc/profiles.d so hadoop/hdfs commands
    are in the path for all users.
    """
    get_logger().debug("Setting up hadoop shell profile on host {}".format(env.host))
    hadoop_profile = 'export PATH="$PATH:{}:{}"'.format(
        os.path.join(install_dir, 'bin'),
        os.path.join(install_dir, 'sbin'))
    script_name = '/etc/profile.d/hadoop.sh'
    # First remove the existing file to avoid potential duplication of path entries.
    sudo('rm -f {}'.format(script_name))
    sudo('echo {} > {}'.format(hadoop_profile, script_name))
    sudo('chmod 644 {}'.format(script_name))


@task
def passwd(username=None, password=None):
    """Changes password. e.g. fab passwd:username=hdfs,password=password"""
    get_logger().debug('changing password for user {}'.format(username))
    with hide('commands', 'output', 'running', 'warnings', 'debug',
              'status'), settings(warn_only=True):
        result = sudo("echo {} | passwd --stdin {}".format(password, username))
    return result


@task
def add_user_task(new_user=None):
    get_logger().debug("Adding user '{}' on host {}".format(new_user.name, env.host))
    with settings(warn_only=True):
        sudo('groupadd -f {}'.format(new_user.group))
        result = sudo('useradd {} -m -g {}'.format(new_user.name, new_user.group))

    if result.succeeded:
        result = passwd(new_user.name, new_user.password)

    if not result.succeeded:
        get_logger().error(
            "Adding user {} on host {} failed - {}".format(
                new_user.name, env.host,
                result.stdout if result.stdout else "No error message available."))
        return False


def add_user(cluster=None, new_user=None):
    """
    Creates an unprivileged user e.g. to run HDFS commands and submit jobs.
    """
    targets = cluster.get_all_hosts()
    get_logger().info("Adding user '{}' on {} hosts".format(
        new_user.name, len(targets)))
    with hide('status', 'warnings', 'running', 'stdout', 'stderr', 'user', 'commands'):
        if not execute(add_user_task, hosts=targets, new_user=new_user):
            get_logger().error('Failed to create user {}.'.format(new_user.name))
            return False
        if cluster.is_kerberized():
            make_headless_principal(cluster, kadmin_util=None, user=new_user)


def is_service_running(service_name):
    """
    Checks if the named Service is running.
    :param service_name:
    :return:
    """
    resultbuf = io.StringIO()
    with settings(warn_only=True, stdout=resultbuf):
        result = sudo('jps')
        resultstring = resultbuf.getvalue()
        resultbuf.close()
    if result.succeeded and resultstring.find(service_name) != -1:
        return True
    else:
        return False


def kill_service(service_name):
    """
    Kills the named service
    :param service_name:
    :return:
    """
    with settings(warn_only=True):
        sudo("kill $(jps | grep %s | awk '{print $1}'" % service_name)
    return True


@task
def kill_scsi_server():
    """
    Kills the jscsi kadmin_server
    :return:
    """
    if is_service_running('Launcher'):
        kill_service('Launcher')


@task
def kill_scm():
    """
    Kills if SCM is running
    :return:
    """
    if is_service_running('StorageContainerManager'):
        kill_service('StorageContainerManager')


@task
def kill_cblock_server():
    """
    Kills the cblock kadmin_server
    :return:
    """
    if is_service_running('CBlockManager'):
        kill_service('CBlockManager')


@task
def start_ozone(cluster):
    """ Starts HDFS and Ozone"""
    get_logger().info("Starting Ozone services ...")
    install_dir = cluster.get_hadoop_install_dir()
    start_dfs_cmd = '{}/sbin/start-ozone.sh'.format(install_dir)
    sudo(start_dfs_cmd, user=constants.HDFS_USER)
    return True


@task
def stop_ozone(cluster):
    """ Stops HDFS and Ozone"""
    get_logger().info("Stopping Ozone services ...")
    install_dir = cluster.get_hadoop_install_dir()
    start_dfs_cmd = '{}/sbin/stop-ozone.sh'.format(install_dir)
    sudo(start_dfs_cmd, user=constants.HDFS_USER)
    return True


@task
def start_dfs(cluster):
    """Starts the dfs cluster. """
    get_logger().info("Starting HDFS services ... this can take some time.")
    install_dir = cluster.get_hadoop_install_dir()
    start_dfs_cmd = '{}/sbin/start-dfs.sh'.format(install_dir)
    sudo(start_dfs_cmd, pty=False)
    return True


@task
def start_yarn(cluster):
    """Starts the yarn services. """
    get_logger().info("Starting YARN services ... this can take some time.")
    install_dir = cluster.get_hadoop_install_dir()
    start_yarn_cmd = '{}/sbin/start-yarn.sh'.format(install_dir)
    sudo(start_yarn_cmd, pty=False, user=constants.YARN_USER)
    return True


@task
def stop_dfs(cluster):
    """Stops the dfs cluster."""
    get_logger().info("Stopping HDFS services ...")
    install_dir = cluster.get_hadoop_install_dir()
    stop_dfs_cmd = '{}/sbin/stop-dfs.sh'.format(install_dir)
    sudo(stop_dfs_cmd, user=constants.HDFS_USER)
    return True


@task
def stop_yarn(cluster):
    """Stops the yarn services."""
    get_logger().info("Stopping YARN services ...")
    install_dir = cluster.get_hadoop_install_dir()
    stop_yarn_cmd = '{}/sbin/stop-yarn.sh'.format(install_dir)
    sudo(stop_yarn_cmd, user=constants.YARN_USER)
    return True


@task
def start_scm(cluster):
    """Starts the storage container manager"""
    get_logger().info("Starting the SCM ...")
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        install_dir = cluster.get_hadoop_install_dir()
        start_scm_cmd = '{}/bin/hdfs --daemon start scm'.format(install_dir)
        sudo(start_scm_cmd, user=constants.HDFS_USER)
    return True


@task
def stop_scm(cluster):
    """Stops the storage container manager"""
    get_logger().info("Stopping the SCM ...")
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        install_dir = cluster.get_hadoop_install_dir()
        stop_scm_cmd = '{}/bin/hdfs --daemon stop scm'.format(install_dir)
        sudo(stop_scm_cmd, user=constants.HDFS_USER)
    return True


@task
def start_cblock_server(cluster):
    """Starts the cBlockServer"""
    get_logger().info("Starting the cBlockServer ...")
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        install_dir = cluster.get_hadoop_install_dir()
        start_cblock_cmd = '{}/bin/hdfs --daemon start cblockserver'.format(install_dir)
        sudo(start_cblock_cmd, user=constants.HDFS_USER)
    return True


@task
def stop_cblock_server(cluster):
    """Stops the cBlockServer"""
    get_logger().info("Stopping the cBlockServer ...")
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        install_dir = cluster.get_hadoop_install_dir()
        stop_cblock_cmd = '{}/bin/hdfs --daemon stop cblockserver'.format(install_dir)
        sudo(stop_cblock_cmd, user=constants.HDFS_USER)
    return True


@task
def start_jscsi_server(cluster):
    """Starts the JSCSI Server"""
    get_logger().info("Starting the JSCSI kadmin_server ...")
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        install_dir = cluster.get_hadoop_install_dir()
        start_jscsi_cmd = '{}/bin/hdfs --daemon start jscsi'.format(install_dir)
        sudo(start_jscsi_cmd, user=constants.HDFS_USER)
    return True


@task
def stop_jscsi_server(cluster):
    """Stops the JSCSI Server"""
    get_logger().info("Stopping the JSCSI kadmin_server ...")
    if cluster.get_config(constants.KEY_OZONE_ENABLED):
        install_dir = cluster.get_hadoop_install_dir()
        stop_jscsi_cmd = '{}/bin/hdfs --daemon stop jscsi'.format(install_dir)
        sudo(stop_jscsi_cmd, user=constants.HDFS_USER)
    return True


if __name__ == '__main__':
    pass
