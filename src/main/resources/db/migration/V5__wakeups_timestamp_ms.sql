
-- Define WAKEUP timestamp in millisecond


ALTER TABLE sched_job_wakeups MODIFY wakeup_time BIGINT NOT NULL;

commit;