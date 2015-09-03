
-- Add a retry count column


ALTER TABLE sched_job_wakeups ADD COLUMN retry_count INT NOT NULL DEFAULT '0';

commit;