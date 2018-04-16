#!/usr/bin/env bash

set -x
pkill -f java -9 
systemctl stop docker

(((
echo t
echo 15
echo w 
) | sudo fdisk /dev/sdb
echo t
echo 15
echo w 
) | sudo fdisk /dev/sdc
echo t
echo 15
echo w 
) | sudo fdisk /dev/sdd

umount /data/disk1
umount /data/disk2
umount /data/disk3

pvcreate /dev/sdb1
pvcreate /dev/sdc1
pvcreate /dev/sdd1

vgcreate vg02 /dev/sdb1
vgextend vg02 /dev/sdc1
vgextend vg02 /dev/sdd1
lvcreate -L 10T vg02 -n docker
lvcreate -L 5T vg02 -n data
mkfs.ext4 /dev/mapper/vg02-docker
mkfs.ext4 /dev/mapper/vg02-data

echo "/dev/mapper/vg02-docker   /var/lib/docker         ext4     defaults        0 0" >> /etc/fstab
echo "/dev/mapper/vg02-data   /data    ext4     defaults        0 0" >> /etc/fstab

mount /var/lib/docker
mount /data
mount -a
