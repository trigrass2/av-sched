
-- Add WAKEUP jobs schema


CREATE TABLE IF NOT EXISTS sched_job_wakeups (
    id VARCHAR(255) PRIMARY KEY,
    wakeup_time TIMESTAMP,
    callback VARCHAR(255)
);

CREATE INDEX IDX_SCHED_WAKEUP_TIME ON sched_job_wakeups(wakeup_time);

commit;