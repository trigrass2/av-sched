
# Build Docker image

```
> cp ../../target/*-exec.jar ./av-sched.jar
> sudo docker build -t airvantage/av-sched .
```

# Load Docker container

```
> sudo docker run -it -rm --name=av-sched -p 8086:8086 -v /etc/av-sched:/etc/av-sched airvantage/av-sched
```

# Configuration

By default the `AVSCHED_CONF_DIR` environment variable references `/etc/av-shed` host directory.
All the properties and the Logback configuration files should be copied into this folder.
