#!/usr/bin/env bash
yum -y install yum-versionlock
yum versionlock docker-ce
yum versionlock docker-ce-selinux
yum -y update
systemctl restart kubelet
