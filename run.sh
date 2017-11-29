#!/bin/bash
echo "
[client]
user=$DB_USER
password=$DB_PASSWORD
host=$DB_HOST
" > /home/$USER/mysql_home/my.cnf
echo "Setup cron"
service rsyslog start
service cron start
tail -f  /var/log/cron.log