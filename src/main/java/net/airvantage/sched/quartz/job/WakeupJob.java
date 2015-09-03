package net.airvantage.sched.quartz.job;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.airvantage.sched.app.ServiceLocator;
import net.airvantage.sched.dao.JobWakeupDao;
import net.airvantage.sched.model.JobWakeup;
import net.airvantage.sched.services.tech.JobExecutionHelper;

@DisallowConcurrentExecution
public class WakeupJob implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(WakeupJob.class);

    private static final int QUERY_LIMIT = 1_000;

    private JobExecutionHelper jobExecutionHelper;
    private JobWakeupDao jobWakeupDao;
    private int threadPoolSize;

    // ------------------------------------------------- Constructors -------------------------------------------------

    /**
     * Constructor used by Quartz to load the job.
     */
    public WakeupJob() {
        this(ServiceLocator.getInstance().geJobExecutionHelper(), ServiceLocator.getInstance().getJobWakeupDao(),
                ServiceLocator.getInstance().getWakeupJobThreadPoolSize());
    }

    protected WakeupJob(JobExecutionHelper jobExecutionHelper, JobWakeupDao jobWakeupDao, int threadPoolSize) {

        this.jobExecutionHelper = jobExecutionHelper;
        this.jobWakeupDao = jobWakeupDao;
        this.threadPoolSize = threadPoolSize;
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
            long now = System.currentTimeMillis();
            ExecutorService executor = null;
            boolean processing = true;

            do {
                List<JobWakeup> wakeups = jobWakeupDao.find(now, QUERY_LIMIT);

                if (wakeups != null && !wakeups.isEmpty()) {
                    // Create executor if needed
                    if (executor == null) {
                        executor = this.buildExecutorService();
                    }

                    processing = processWakeups(executor, wakeups);
                } else {
                    processing = false;
                }
            } while (processing);

            if (executor != null) {
                executor.shutdownNow();
            }

        } catch (Exception ex) {
            LOG.error("Unable to execute WAKEUP job " + key, ex);
            throw new JobExecutionException("Unable to execute WAKEUP job " + key, ex);
        }
    }

    /**
     * Process the list of {@link JobWakeup}.
     * 
     * @return true if all {@link JobWakeup} have been processed successfully
     */
    private boolean processWakeups(ExecutorService executor, List<JobWakeup> wakeups) throws Exception {
        for (JobWakeup wakeup : wakeups) {
            try {
                executor.execute(() -> {
                    this.jobExecutionHelper.execute(wakeup);
                });

            } catch (RejectedExecutionException reex) {
                LOG.warn("The thread pool queue is full, remaining wake-ups will be processed later");
            }
        }

        return executor.awaitTermination(10, TimeUnit.MINUTES);
    }

    /**
     * Returns an instance of {@link ExecutorService}
     */
    private ExecutorService buildExecutorService() {

        ThreadPoolExecutor executor = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(QUERY_LIMIT));

        // if the pool is full the submit call will throw a RejectedExecutionException
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());

        return executor;
    }
}