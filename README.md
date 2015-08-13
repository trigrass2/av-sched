# AirVantage scheduling micro service

## Work In Progress

- Some API validation missing
- Jobs are not "killed" if calling them fails too often

## Configuration

The AVSCHED_CONF_DIR environment property should be set to the location of a configuration folder.
It should contain a "deploy-sched-local.properties" file.
A template exist in the "conf" folder.

### Mandatory

- `av-sched.secret` : secret string exchanged in all API calls. (See [Security](#Security) )
- `av-sched.db.server`
- `av-sched.db.port`
- `av-sched.db.dbName`
- `av-sched.db.user`
- `av-sched.db.password`

Database credentials. You should create the db and user yourself.

### Optional

- `av-sched.port` : 8086 by default.

## Usage

### Build UI

- `npm install -g gulp`
- `npm install -g bower`
- `bower install`
- `gulp build` (or `gulp dev` if you want js files to be recompiled on the fly)

### Run server

Database bootstrap and migrations are automatically run at application startup.

#### From eclipse

- `mvn eclipse:eclipse`
- setup AVSCHED_CONF_DIR variable in build conf
- Run `Launcher`

#### From jar

- `mvn package`
- `export AVSCHED_CONF_DIR=/home/....`
- `java -jar target/av-sched-x.y.z-exec.jar`

### Options

- `--clear` : clear data from MySql tables and Quartz scheduler (usefull for tests.)

## UI

Open `http://localhost:8086/sched/`.

## API

## Security

All api calls to `av-sched` should contain the header `X-Sched-secret`, with the value of
the `av-sched.secret` parameter.

The application that implements the task should also check that this header has the same value (and return a 401 otherwise.)

Obviously, this secret should remain, *ahem*, [secret](http://uncyclopedia.wikia.com/wiki/Captain_Obvious).

### Schedule a CRON job

~~~
POST host:8086/sched/api/job-def
{
  "config" : {
   "id" : "av-server/timers",
   "url" : "http://murphy:3000/echo",
   "timeout" : 60000
  },
  "scheduling" : {
    "type" : "cron",
    "value" : "0/30 0/1 * 1/1 * ? *"
  }
}
~~~

### Schedule a WAKEUP job

~~~
POST host:8086/sched/api/job-def
{
  "config" : {
   "id" : "av-server/timers",
   "url" : "http://murphy:3000/echo"
  },
  "scheduling" : {
    "type" : "wakeup",
    "value" : 1435579200000
  }
}
~~~

A WAKEUP job can be triggered with a certain delay (many seconds or more).
These kind of jobs are executed with a limited thread pool and the delay depends on the execution time of each job.
When a WAKEUP job execution fails then a retry is done with an exponential wait time.

### Unschedule a job

~~~
DELETE host:8086/sched/api/job-def
{
  "id" : "av-server/timers"
}
~~~

Return value :

~~~
{
  "id" : "av-server/timers",
  "deleted" : true
}
~~~

Deleting a job that do not exist simply returns "false" in the "deleted" field.

### Ack a job

Acknowledge a "locked" job (one that has been triggered by the server.)

~~~
POST host:8086/sched/api/job-action/ack
{
  "id" : "av-server/timers"
}
~~~

### Trigger a job

~~~
POST host:8086/sched/api/job-action/trigger
{
  "id" : "av-server/timers"
}
~~~

Response:

~~~
{
  "triggered" : true
}
~~~

A job that is locked (has been fired but not acknowledged yet) will not be triggered.

### List scheduled jobs

~~~
GET host:8086/sched/api/job
[ {
  "config" : {
    "id" : "test-job-1426783470991",
    "url" : "http://localhost:3030/test/test-job-1426783470991",
    "timeout" : 60000
  },
  "scheduling" : {
    "type" : "cron",
    "startAt" : 1435579200000,
    "value" : ".."
  },
  "lock" : {
    "locked" : true,
    "expiresAt" : 1426783532000,
    "expired" : true
  }
}, {
  "config" : {
    "id" : "test-job-1426783460784",
    "url" : "http://localhost:3020/test/test-job-1426783460784",
    "timeout" : 60000
   },
  "scheduling" : {
    "type" : "cron",
    "value" : ".."
  },
  "lock" : {
    "locked" : true,
    "expiresAt" : 1426783530000,
    "expired" : true
  }
} ]
~~~

### Get a single job

~~~
GET host:8086/sched/api/job?jobId=test-job-1426783470991
[ {
  "config" : {
    "id" : "test-job-1426783470991",
    "url" : "http://localhost:3030/test/test-job-1426783470991",
    "timeout" : 60000
  },
  "scheduling" : {
    "type" : "cron",
    "startAt" : 1435579200000,
    "value" : ".."
  },
  "lock" : {
    "locked" : true,
    "expiresAt" : 1426783532000,
    "expired" : true
  }
} ]
~~~

### Callback

~~~
POST localhost:3030/test/test-job-1426783470991
~~~

Response (directive can be returned by the callback) :

~~~
{
  "ack" : true,
  "retry" : 30000 // The time to wait before the next fire
}
~~~

## Functionnal Tests

See src/node/README.md
