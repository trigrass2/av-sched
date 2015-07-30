package net.airvantage.sched.quartz;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.model.JobLock;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.services.JobStateService;

import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A listener to apply lock policy : no job will be executed if a lock is already active.
 */
public class DefaultTriggerListener extends TriggerListenerSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultTriggerListener.class);

    private JobStateService jobStateService;

    @Override
    public String getName() {
        return "lockTriggerListener";
    }

    public DefaultTriggerListener(JobStateService jobStateService) {

        this.jobStateService = jobStateService;
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {

        try {
            String jobId = context.getJobDetail().getKey().getName();
            JobState jobState = this.jobStateService.find(jobId);
            if (jobState != null) {

                // Do not execute a job if the remote client has not acknowledged the previous run
                JobLock lock = jobState.getLock();
                if ((lock != null) && lock.isLocked()) {
                    if (lock.isExpired()) {

                        this.jobStateService.unlockJob(jobId);
                        return false;

                    } else {
                        return true;
                    }
                }
            }

        } catch (AppException e) {
            LOG.error("Unable to veto job execution", e);
        }
        return false;
    }

}
