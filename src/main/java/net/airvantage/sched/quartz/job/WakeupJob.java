package net.airvantage.sched.quartz.job;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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

            while (processing) {
                long start = System.currentTimeMillis();
                List<JobWakeup> wakeups = jobWakeupDao.find(now, QUERY_LIMIT);

                if (wakeups != null && !wakeups.isEmpty()) {
                    // Create executor if needed
                    if (executor == null) {
                        executor = this.buildExecutorService();
                    }

                    processWakeups(executor, wakeups);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} wakeups have been processed in {} ms", wakeups.size(),
                                (System.currentTimeMillis() - start));
                    }
                } else {
                    processing = false;
                }
            }

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
     */
    private void processWakeups(ExecutorService executor, List<JobWakeup> wakeups) throws Exception {
        List<Callable<Void>> callables = new ArrayList<>();
        for (JobWakeup wakeup : wakeups) {
            callables.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    jobExecutionHelper.execute(wakeup);
                    return null;
                }
            });
        }

        List<Future<Void>> futures = executor.invokeAll(callables);
        for (Future<Void> future : futures) {
            // Wait for completion
            future.get();
        }
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