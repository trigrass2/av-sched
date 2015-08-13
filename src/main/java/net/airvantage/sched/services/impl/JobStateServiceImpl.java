package net.airvantage.sched.services.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.app.exceptions.AppExceptions;
import net.airvantage.sched.dao.JobConfigDao;
import net.airvantage.sched.dao.JobLockDao;
import net.airvantage.sched.dao.JobSchedulingDao;
import net.airvantage.sched.model.JobConfig;
import net.airvantage.sched.model.JobLock;
import net.airvantage.sched.model.JobScheduling;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.services.JobStateService;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service to manage the jobs state.
 * 
 * <p>
 * The state of a job is the current status built from the job configuration and scheduling configuration.
 * <p>
 */
public class JobStateServiceImpl implements JobStateService {

    private static final Logger LOG = LoggerFactory.getLogger(JobStateServiceImpl.class);

    private JobLockDao jobLockDao;
    private JobConfigDao jobConfigDao;
    private JobSchedulingDao jobSchedulingDao;

    // ------------------------------------------------ Constructors --------------------------------------------------

    public JobStateServiceImpl(JobConfigDao jobConfigDao, JobLockDao jobLockDao, JobSchedulingDao jobSchedulingDao) {

        this.jobLockDao = jobLockDao;
        this.jobConfigDao = jobConfigDao;
        this.jobSchedulingDao = jobSchedulingDao;
    }

    // ------------------------------------------- JobStateService Methods --------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public JobState find(String id) throws AppException {

        JobConfig config = null;
        JobLock lock = null;
        JobState state = null;
        JobScheduling scheduling = null;

        try {
            config = this.jobConfigDao.find(id);
            lock = this.jobLockDao.find(id);
            scheduling = this.jobSchedulingDao.find(id);

        } catch (SQLException e) {
            LOG.error(String.format("Unable to find job state with id", id), e);
            throw AppExceptions.serverError(e);

        } catch (SchedulerException e) {
            LOG.error(String.format("Unable to find job state with id", id), e);
            throw AppExceptions.serverError(e);
        }

        if (config != null) {
            state = new JobState();
            state.setConfig(config);
            state.setLock(lock);
            state.setScheduling(scheduling);
        }

        return state;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<JobState> findAll() throws AppException {

        List<JobState> states = new ArrayList<JobState>();

        try {
            Map<String, JobConfig> jobConfigs = this.jobConfigDao.findAll();
            Map<String, JobLock> jobLocks = this.jobLockDao.findAll();
            Map<String, JobScheduling> jobSchedulings = this.jobSchedulingDao.findAll();

            for (Entry<String, JobConfig> entry : jobConfigs.entrySet()) {
                String id = entry.getKey();
                JobConfig jobConfig = entry.getValue();

                JobState state = new JobState();
                state.setConfig(jobConfig);

                if (jobLocks.containsKey(id)) {
                    JobLock jobLock = jobLocks.get(id);
                    state.setLock(jobLock);
                } else {
                    state.setLock(new JobLock());
                }

                if (jobSchedulings.containsKey(id)) {
                    JobScheduling jobScheduling = jobSchedulings.get(id);
                    state.setScheduling(jobScheduling);
                } else {
                    state.setScheduling(new JobScheduling());
                }

                states.add(state);

            }

        } catch (SQLException e) {
            LOG.error(String.format("Unable to find job states"), e);
            throw AppExceptions.serverError(e);

        } catch (SchedulerException e) {
            LOG.error(String.format("Unable to find job states"), e);
            throw AppExceptions.serverError(e);
        }

        return states;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void lockJob(String id) throws AppException {

        try {
            JobState jobState = find(id);
            if (jobState != null) {
                long expiresAt = System.currentTimeMillis() + jobState.getConfig().getTimeout();
                LOG.debug("Will save expiration date" + expiresAt);
                this.jobLockDao.add(id, expiresAt);
            }

        } catch (SQLException e) {
            LOG.error(String.format("Unable to lock job state with id", id), e);
            throw AppExceptions.serverError(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unlockJob(String id) throws AppException {

        try {
            JobState jobState = find(id);
            if (jobState == null) {
                throw AppExceptions.jobNotFound(id);

            } else {
                this.jobLockDao.delete(id);
            }

        } catch (SQLException e) {
            LOG.error(String.format("Unable to lock job state with id", id), e);
            throw AppExceptions.serverError(e);
        }
    }

}
