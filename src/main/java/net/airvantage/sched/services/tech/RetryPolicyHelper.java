package net.airvantage.sched.services.tech;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.dao.JobWakeupDao;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.model.JobWakeup;
import net.airvantage.sched.quartz.job.JobResult;
import net.airvantage.sched.quartz.job.JobResult.CallbackStatus;
import net.airvantage.sched.services.JobSchedulingService;
import net.airvantage.sched.services.JobStateService;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service to apply the retry policy.
 */
public class RetryPolicyHelper {

    private static final Logger LOG = LoggerFactory.getLogger(RetryPolicyHelper.class);

    public static final long DEFAULT_ERROR_DELAY_MS = TimeUnit.MINUTES.toMillis(1);
    public static final long DEFAULT_RETRY_DELAY_MS = TimeUnit.SECONDS.toMillis(12);

    public static final int DEFAULT_MAX_NB_RETRIES_PER_JOB = 100;
    public static final int DEFAULT_MAX_NB_RETRIES = 5_000;

    /**
     * Map to record the current number of retries by job key.
     */
    private ConcurrentHashMap<String, AtomicInteger> retries = new ConcurrentHashMap<>();

    private JobWakeupDao jobWakeupDao;
    private JobStateService jobStateService;
    private JobSchedulingService jobSchedulingService;

    // ----------------------------------------------- Constructors ---------------------------------------------------

    public RetryPolicyHelper(JobStateService jobStateService, JobSchedulingService jobSchedulingService,
            JobWakeupDao jobWakeupDao) {

        this.jobWakeupDao = jobWakeupDao;
        this.jobStateService = jobStateService;
        this.jobSchedulingService = jobSchedulingService;
    }

    // ----------------------------------------------- Public Methods -------------------------------------------------

    /**
     * Handle the result of a CRON job.
     */
    public void handleResult(JobResult result) throws AppException {
        LOG.debug("handleResult : result={}", result);

        Validate.notNull(result);
        Validate.notNull(result.getJobId());

        JobState state = this.jobStateService.find(result.getJobId());
        if (state != null) {

            if (result.getStatus() == CallbackStatus.FAILURE) {
                this.callbackFailure(state, result);

            } else {
                this.callbackSuccess(state, result);
            }
        }
    }

    /**
     * Handle the result of a WAKEUP job.
     */
    public void handleResult(JobWakeup wakeup, JobResult result) {
        LOG.debug("handleResult : wakeup={}, result={}", wakeup, result);

        long delay = 0;
        if (result.getRetry() > 0) {
            delay = this.getRetryDelay(wakeup.getId(), result.getRetry());

        } else if (result.getStatus() == CallbackStatus.FAILURE) {
            delay = this.getRetryDelay(wakeup.getId(), DEFAULT_ERROR_DELAY_MS);
        }

        if (delay > 0) {
            wakeup.setWakeupTime(System.currentTimeMillis() + delay);
            jobWakeupDao.persist(wakeup);

        } else {
            jobWakeupDao.delete(wakeup.getId());
            retries.remove(wakeup.getId());
        }
    }

    // ----------------------------------------------- Private Methods ------------------------------------------------

    private void callbackSuccess(JobState state, JobResult result) throws AppException {

        boolean ack = result.isAck();
        if (ack) {

            this.jobSchedulingService.ackJob(result.getJobId());
        }

        // TODO manage requested retry
    }

    private void callbackFailure(JobState state, JobResult result) throws AppException {

        // TODO manage retry
        // TODO check the retry date is before the next cron trigger fire time
    }

    private long getRetryDelay(String jobId, long expected) {

        AtomicInteger count = new AtomicInteger();
        if (retries.size() < DEFAULT_MAX_NB_RETRIES) {
            
            count = retries.putIfAbsent(jobId, new AtomicInteger());
            if (count == null) {
                count = retries.get(jobId);
            }
            
        } else {
            LOG.warn("The cache of job retry counters is full ({} values).", DEFAULT_MAX_NB_RETRIES);
        }
        
        long delay = expected + (count.getAndIncrement() * DEFAULT_RETRY_DELAY_MS);
        if (count.get() > DEFAULT_MAX_NB_RETRIES_PER_JOB) {
            LOG.error("The job {} retried more than {} times !", jobId, DEFAULT_MAX_NB_RETRIES_PER_JOB);
        }

        return delay;
    }

}
