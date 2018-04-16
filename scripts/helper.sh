#!/usr/bin/env bash
op="$1"
case $op in
  "login")
    echo "iscsiadm -m node -T $2 --login"
    iscsiadm -m node -T $2 --login
  ;;
  "logout")
    echo "iscsiadm -m node --logoutall=all"
    iscsiadm -m node --logoutall=all
  ;;
  "logout1")
    echo "iscsiadm -m node -u -T $2 -p 127.0.0.1:3260"
    iscsiadm -m node -u -T $2 -p 127.0.0.1:3260
  ;;
  "add")
    echo "iscsiadm -m node -o new -T $2 -p 127.0.0.1"
    iscsiadm -m node -o new -T $2 -p 127.0.0.1
  ;;
  "grep")
    echo "grep \"Attached SCSI\" /var/log/messages"
    grep "Attached SCSI" /var/log/messages
  ;;
  "del")
    echo "iscsiadm -m node -o delete -T $2"
    iscsiadm -m node -o delete -T $2
  ;;
  "show")
    echo "iscsiadm -m session"
    iscsiadm -m session
  ;;
  "help")
    echo "A simple bash command wrapper to make jscsi commands easier"
    echo "helper.sh login user:volume   -- to login to the volume user:volume"
    echo "helper.sh logout              -- to logout all logged in volumes"
    echo "helper.sh logout1 user:volume -- to logout a specific volume"
    echo "helper.sh grep                -- the last line shows the most recent logged in volume"
    echo "helper.sh add user:volume     -- add a new volume to local record"
    echo "helper.sh del user:volume     -- delete a volume from local record"  
  ;;
esac
