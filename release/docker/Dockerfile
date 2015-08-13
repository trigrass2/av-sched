FROM airvantage/av-java

EXPOSE 8086

ENTRYPOINT /usr/bin/java -DAVSCHED_CONF_DIR=/etc/av-sched/ -Dlogback.configurationFile=/etc/av-sched/logback.xml -jar *.jar