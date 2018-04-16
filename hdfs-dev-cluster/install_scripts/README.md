These are helper scripts top partition, install and upgrade kubernetes nodes.

Usually they are copied to host with psshscp:

`
psshscp -l melek  -h <(cat hosts) ./upgradeto19.sh /tmp/
`
