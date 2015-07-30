package net.airvantage.sched.quartz.job;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.airvantage.sched.app.ServiceLocator;
import net.airvantage.sched.dao.JobWakeupDao;
import net.airvantage.sched.model.JobWakeup;
import net.airvantage.sched.services.tech.JobExecutionHelper;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DisallowConcurrentExecution
public class WakeupJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(WakeupJob.class);

    private JobExecutionHelper jobExecutionHelper;
    private JobWakeupDao jobWakeupDao;

    private int threadPoolSize = 8;
    private int maxQueueSize = 10_000;

    // ------------------------------------------------- Constructors -------------------------------------------------

    /**
     * Constructor used by Quartz to load the job.
     */
    public WakeupJob() {
        this(ServiceLocator.getInstance().geJobExecutionHelper(), ServiceLocator.getInstance().getJobWakeupDao());
    }

    protected WakeupJob(JobExecutionHelper jobExecutionHelper, JobWakeupDao jobWakeupDao) {

        this.jobExecutionHelper = jobExecutionHelper;
        this.jobWakeupDao = jobWakeupDao;
    }

    // ------------------------------------------------- Public Methods -----------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOG.debug("execute : context={}", context);

        JobKey key = context.getJobDetail().getKey();
        try {

            ExecutorService executor = this.buildExecutorService();
            jobWakeupDao.iterate(0, System.currentTimeMillis(), (JobWakeup wakeup) -> {

                try {
                    executor.execute(() -> {
                        this.jobExecutionHelper.execute(wakeup);
                    });

                    return true;

                } catch (RejectedExecutionException reex) {
                    LOG.warn("The thread pool queue is full, remaining wake-ups will be processed later");
                    return false;
                }
            });

            executor.shutdown();
            executor.awaitTermination(12, TimeUnit.HOURS);

        } catch (Exception ex) {
            LOG.error("Unable to execute WAKEUP job " + key, ex);
            throw new JobExecutionException("Unable to execute WAKEUP job " + key, ex);
        }
    }

    private ExecutorService buildExecutorService() {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(maxQueueSize));

        // if the pool is full the submit call will throw a RejectedExecutionException
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());

        return executor;
    }

}
