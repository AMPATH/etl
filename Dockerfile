FROM ubuntu:16.04
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    cron mysql-client rsyslog\
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY . /opt/etl
COPY etl-scripts-cron /etc/cron.d/etl-scripts-cron
RUN echo "cron.* /var/log/cron.log" >> /etc/rsyslog.conf
RUN chmod +x /opt/etl/run.sh
RUN chmod 0644 /etc/cron.d/etl-scripts-cron
RUN mkdir /home/$USER/mysql_home
ENV MYSQL_HOME=/home/$USER/mysql_home
RUN touch /home/$USER/etl_log
CMD /opt/etl/run.sh