package net.airvantage.sched.services.impl;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.math.NumberUtils;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.dao.JobConfigDao;
import net.airvantage.sched.dao.JobLockDao;
import net.airvantage.sched.dao.JobSchedulingDao;
import net.airvantage.sched.dao.JobWakeupDao;
import net.airvantage.sched.model.JobConfig;
import net.airvantage.sched.model.JobDef;
import net.airvantage.sched.model.JobLock;
import net.airvantage.sched.model.JobScheduling;
import net.airvantage.sched.model.JobSchedulingType;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.model.JobWakeup;
import net.airvantage.sched.quartz.job.CronJob;
import net.airvantage.sched.quartz.job.WakeupJob;
import net.airvantage.sched.services.JobSchedulingService;
import net.airvantage.sched.services.JobStateService;

/**
 * A service to manage the jobs scheduling.
 */
public class JobSchedulingServiceImpl implements JobSchedulingService {

    private Logger LOG = LoggerFactory.getLogger(JobSchedulingServiceImpl.class);

    private Scheduler scheduler;
    private JobStateService jobStateService;

    private JobLockDao jobLockDao;
    private JobConfigDao jobConfigDao;
    private JobWakeupDao jobWakeupDao;
    private JobSchedulingDao jobSchedulingDao;

    private String jobWakeupCron;

    // ------------------------------------------------ Constructors --------------------------------------------------

    public JobSchedulingServiceImpl(Scheduler scheduler, JobStateService jobStateService, JobConfigDao jobConfigDao,
            JobLockDao jobLockDao, JobSchedulingDao jobSchedulingDao, JobWakeupDao jobWakeupDao, String jobWakeupCron) {

        this.scheduler = scheduler;
        this.jobStateService = jobStateService;

        this.jobLockDao = jobLockDao;
        this.jobConfigDao = jobConfigDao;
        this.jobWakeupDao = jobWakeupDao;
        this.jobSchedulingDao = jobSchedulingDao;
        this.jobWakeupCron = jobWakeupCron;
    }

    public void loadInternalJobs() throws AppException {

        try {
            JobDef jobDef = new JobDef();

            JobConfig jobConfig = new JobConfig();
            jobDef.setConfig(jobConfig);
            jobConfig.setId("internal/wakeup-jobs-timer");

            JobScheduling jobScheduling = new JobScheduling();
            jobDef.setScheduling(jobScheduling);
            jobScheduling.setType(JobSchedulingType.CRON);
            jobScheduling.setValue(jobWakeupCron);

            scheduleQuarzJob(jobDef, WakeupJob.class);

        } catch (Exception ex) {
            LOG.error("Unable to load internal jobs", ex);
            throw new AppException("load.internal.jobs.error", ex);
        }
    }

    // ------------------------------------------ JobSchedulingService Methods ----------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public void scheduleJob(JobDef jobDef) throws AppException {
        LOG.debug("scheduleJob : jobDef={}", jobDef);

        Validate.notNull(jobDef);
        validate(jobDef.getConfig());
        validate(jobDef.getScheduling());

        try {
            if (jobDef.getScheduling().getType() == JobSchedulingType.WAKEUP) {

                JobWakeup wakeup = new JobWakeup();
                wakeup.setId(jobDef.getConfig().getId());
                wakeup.setCallback(jobDef.getConfig().getUrl());
                wakeup.setWakeupTime(new Long(jobDef.getScheduling().getValue()));

                jobWakeupDao.persist(wakeup);

            } else {

                scheduleQuarzJob(jobDef, CronJob.class);

                // Persist the job configuration
                this.jobConfigDao.persist(jobDef.getConfig());
            }

        } catch (Exception ex) {
            LOG.error("Unable to schedule job " + jobDef, ex);
            throw new AppException("schedule.job.error", Arrays.asList(jobDef.getConfig().getId()), ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rescheduleJob(String jobId, JobScheduling conf) throws AppException {
        LOG.debug("rescheduleJob : id={}, conf={}", jobId, conf);

        validate(conf);

        try {
            Trigger trigger = this.buildTrigger(jobId, conf, null);
            this.scheduler.rescheduleJob(trigger.getKey(), trigger);

        } catch (Exception e) {
            LOG.error("Unable to re-schedule job " + jobId + " with configuration " + conf, e);
            throw new AppException("reschedule.job.error", Arrays.asList(jobId), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean unscheduleJob(String jobId) throws AppException {
        LOG.debug("unscheduleJob : jobId={}", jobId);

        boolean res = false;
        try {

            res = unscheduleQuartzJob(jobId);

            this.jobConfigDao.delete(jobId);
            this.jobLockDao.delete(jobId);

        } catch (Exception ex) {
            LOG.error(String.format("Unable to unschedule job {}", jobId), ex);
            throw new AppException("unchedule.job.error", Arrays.asList(jobId), ex);
        }

        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void ackJob(String jobId) throws AppException {
        LOG.debug("ackJob : jobId={}", jobId);

        try {

            JobConfig config = this.jobConfigDao.find(jobId);
            if (config == null) {
                throw new AppException("job.not.found", Arrays.asList(jobId));
            }

            JobScheduling schedConf = this.jobSchedulingDao.find(jobId);
            if (schedConf == null) {

                // Remove job configuration when a job is complete
                this.unscheduleJob(jobId);

            } else {
                this.jobStateService.unlockJob(jobId);
            }

        } catch (AppException aex) {
            throw aex;

        } catch (Exception sex) {
            LOG.error(String.format("Unable to acknowledge job {}", jobId), sex);
            throw new AppException("ack.job.error", Arrays.asList(jobId), sex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean triggerJob(String jobId) throws AppException {
        LOG.debug("triggerJob : jobId={}", jobId);

        boolean res = false;
        try {

            JobState jobState = this.jobStateService.find(jobId);
            if (jobState == null) {
                throw new AppException("job.not.found", Arrays.asList(jobId));
            }

            JobLock lock = jobState.getLock();

            // Currently locked jobs should not be re-triggered
            if (lock.isLocked() && !lock.isExpired()) {
                res = false;

            } else {
                this.scheduler.triggerJob(this.buildJobKey(jobId));
                res = true;
            }

        } catch (AppException aex) {
            throw aex;

        } catch (Exception ex) {
            LOG.error(String.format("Unable to trigger job {}", jobId), ex);
            throw new AppException("trigger.job.error", Arrays.asList(jobId), ex);
        }

        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearJobs() throws AppException {
        LOG.debug("clearJobs");

        try {
            // Delete configuration
            this.jobLockDao.deleteAll();
            this.jobConfigDao.deleteAll();
            this.jobWakeupDao.deleteAll();

            // Delete scheduling
            this.scheduler.clear();

        } catch (Exception ex) {
            LOG.error("Unable to clear jobs", ex);
            throw new AppException("clear.jobs.error", ex);
        }
    }

    // ----------------------------------------------- Private Methods ------------------------------------------------

    private void validate(JobConfig jobConfig) throws AppException {

        Validate.notNull(jobConfig);

        if (StringUtils.isEmpty(jobConfig.getId())) {
            jobConfig.setId(UUID.randomUUID().toString());
        }

        if (StringUtils.isEmpty(jobConfig.getUrl())) {
            throw new AppException("missing.callback.url");
        }

        if (jobConfig.getUrl().length() > 255) {
            throw new AppException("too.long.url");
        }
    }

    private void validate(JobScheduling scheduling) throws AppException {

        Validate.notNull(scheduling);

        if (scheduling.getType() == null) {
            throw new AppException("missing.scheduling.type");
        }

        if (scheduling.getType() == JobSchedulingType.CRON) {
            if (StringUtils.isEmpty(scheduling.getValue()))
                throw new AppException("missing.schedule.value");

            try {
                CronExpression.validateExpression(scheduling.getValue());
            } catch (ParseException e) {
                throw new AppException("invalid.schedule.value", Arrays.asList(scheduling.getValue()));
            }
        }

        if (scheduling.getType() == JobSchedulingType.WAKEUP) {
            if (StringUtils.isEmpty(scheduling.getValue()))
                throw new AppException("missing.schedule.value");

            if (!NumberUtils.isDigits(scheduling.getValue()))
                throw new AppException("invalid.schedule.value");
        }
    }

    private void scheduleQuarzJob(JobDef jobDef, Class<? extends Job> type) throws SchedulerException {

        JobDetail job = this.buildJob(jobDef.getConfig(), type);
        Trigger trigger = this.buildTrigger(jobDef.getConfig().getId(), jobDef.getScheduling(), job.getKey());

        // Add the new jobs
        // if (!this.scheduler.checkExists(job.getKey())) {
        // TMP MIGRATION - Update each time to update Job.class
        this.scheduler.addJob(job, true);
        // }

        // Add the new triggers or update existing ones
        try {
            if (this.scheduler.checkExists(trigger.getKey())) {
                this.scheduler.rescheduleJob(trigger.getKey(), trigger);

            } else {
                this.scheduler.scheduleJob(trigger);
            }

        } catch (ObjectAlreadyExistsException e) {
            LOG.info("Trigger already exists with key {}, try to replace it.", trigger.getKey());

            // To manage concurrent calls
            this.scheduler.rescheduleJob(trigger.getKey(), trigger);
        }
    }

    /**
     * Try unscheduling the job.
     * 
     * @return true if the job existed, and was unscheduled.
     */
    private boolean unscheduleQuartzJob(String jobId) throws SchedulerException {

        return this.scheduler.deleteJob(this.buildJobKey(jobId));
    }

    private TriggerKey buildTiggerKey(String confId) {
        return new TriggerKey(confId, confId + "-trigger");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Trigger buildTrigger(String confId, JobScheduling conf, JobKey job) {

        TriggerKey key = this.buildTiggerKey(confId);
        TriggerBuilder trigger = TriggerBuilder.newTrigger().withIdentity(key);

        // Set CRON value
        if (StringUtils.isNotEmpty(conf.getValue())) {
            trigger.withSchedule(CronScheduleBuilder.cronSchedule(conf.getValue()));
        }

        // Set DATE value
        if (conf.getStartAt() > 0) {
            trigger.startAt(new Date(conf.getStartAt()));
        }

        if (job != null) {
            trigger.forJob(job);
        }

        return trigger.build();
    }

    private JobKey buildJobKey(String confId) {
        return new JobKey(confId);
    }

    private JobDetail buildJob(JobConfig jobConf, Class<? extends Job> type) {

        JobKey key = this.buildJobKey(jobConf.getId());
        JobBuilder job = JobBuilder.newJob(type).withIdentity(key).storeDurably();

        return job.build();
    }

}
