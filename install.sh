#!/bin/bash

echo "Configure deploy key"
cp ./id_rsa.pub /root/.ssh/
chmod 700 /root/.ssh/id_rsa.pub
chown -R root:root /root/.ssh
