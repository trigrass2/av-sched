package net.airvantage.sched.quartz.job;

import net.airvantage.sched.app.ServiceLocator;
import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.services.tech.JobExecutionHelper;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CronJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(CronJob.class);

    private JobExecutionHelper jobExecutionHelper;

    // ------------------------------------------------- Constructors -------------------------------------------------

    /**
     * Constructor used by Quartz to load the job.
     */
    public CronJob() {
        this(ServiceLocator.getInstance().geJobExecutionHelper());
    }

    protected CronJob(JobExecutionHelper jobExecutionHelper) {
        this.jobExecutionHelper = jobExecutionHelper;
    }

    // ------------------------------------------------- Public Methods -----------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOG.debug("execute : context={}", context);

        JobKey key = context.getJobDetail().getKey();
        String jobId = key.getName();
        JobResult result = null;

        try {
            result = this.jobExecutionHelper.execute(jobId);

        } catch (AppException aex) {
            LOG.error("Unable to execute CRON job " + key + ")", aex);
            throw new JobExecutionException("Unable to execute CRON job " + key, aex);
        }

        context.setResult(result);
    }

}
