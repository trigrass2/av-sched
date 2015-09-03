package net.airvantage.sched.services.tech;

import static net.airvantage.sched.quartz.job.JobResult.CallbackStatus.SUCCESS;
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

    private final static long MAX_RETRY_COUNT = 32; // should lead to 24hours
    private final static long MIN_RETRY_DELAY = 1000L;
    private final static long MAX_RETRY_DELAY = 60 * 60 * 1000L;

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

        long requestedRetryDate = result.getRetry();

        if ((result.getStatus() == SUCCESS && requestedRetryDate <= 0) || (wakeup.getRetryCount() >= MAX_RETRY_COUNT)) {
            // delete
            LOG.trace("handleResult deleting : wakeup={}, result={}", wakeup, result);
            jobWakeupDao.delete(wakeup.getId());
        } else {
            // rescheduling
            int retryCount = wakeup.getRetryCount() + 1;
            long retryDelay = Math.max(requestedRetryDate, computeRetryDelay(retryCount));

            wakeup.setRetryCount(retryCount);
            wakeup.setWakeupTime(retryDelay + System.currentTimeMillis());
            LOG.trace("handleResult rescheduling : wakeup={}, result={}", wakeup, result);
            jobWakeupDao.persist(wakeup);
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

    private long computeRetryDelay(int retryCount) {
        return Math.min(MAX_RETRY_DELAY, Math.max(MIN_RETRY_DELAY, (long) Math.pow(2, retryCount)));
    }

}
