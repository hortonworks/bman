#!/usr/bin/env bash

set -e

cat <<EOF >  /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-ip6tables = 1
net.bridge.bridge-nf-call-iptables = 1
EOF
sysctl --system


yum remove docker-ce
yum remove container-selinux
yum install -y --setopt=obsoletes=0  docker-ce-17.03.1.ce-1.el7.centos   docker-ce-selinux-17.03.1.ce-1.el7.centos

swapoff -a

cat <<EOF > /etc/docker/daemon.json
{
	"exec-opts": ["native.cgroupdriver=systemd"]
}
EOF

sudo systemctl restart docker

cat <<EOF > /etc/yum.repos.d/kubernetes.repo
[kubernetes]
name=Kubernetes
baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOF
setenforce 0
yum install -y kubelet kubeadm kubectl
systemctl enable kubelet && systemctl start kubelet

systemctl enable kubelet.service
systemctl enable docker.service

