package net.airvantage.sched.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.airvantage.sched.model.JobScheduling;
import net.airvantage.sched.model.JobSchedulingType;

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;

/**
 * DAO to manage the {@link JobScheduling} object model. The properties used here are managed by Quartz service.
 */
public class JobSchedulingDao {

    private Scheduler scheduler;

    public JobSchedulingDao(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * Return the scheduling configuration identified by the given identifier.
     */
    public JobScheduling find(String jobId) throws SchedulerException {

        JobScheduling jobScheduling = null;

        List<? extends Trigger> triggersOfJob = scheduler.getTriggersOfJob(new JobKey(jobId));
        if (triggersOfJob != null && !triggersOfJob.isEmpty()) {

            Trigger trigger = triggersOfJob.get(0);
            JobSchedulingWithId sched = fromTrigger(trigger);
            if (sched != null) {
                jobScheduling = sched.jobScheduling;
            }
        }

        return jobScheduling;
    }

    /**
     * Return all the existing scheduling configurations group by their identifier.
     */
    public Map<String, JobScheduling> findAll() throws SchedulerException {
        HashMap<String, JobScheduling> jobSchedulings = new HashMap<String, JobScheduling>();

        Set<TriggerKey> triggerKeys = scheduler.getTriggerKeys(GroupMatcher.anyTriggerGroup());
        for (TriggerKey key : triggerKeys) {
            Trigger trigger = scheduler.getTrigger(key);
            JobSchedulingWithId sched = fromTrigger(trigger);
            if (sched != null) {
                jobSchedulings.put(sched.jobId, sched.jobScheduling);
            }
        }

        return jobSchedulings;
    }

    private class JobSchedulingWithId {
        public String jobId;
        public JobScheduling jobScheduling;

        public JobSchedulingWithId(String jobId, JobScheduling jobScheduling) {
            super();
            this.jobId = jobId;
            this.jobScheduling = jobScheduling;
        }

    }

    private JobSchedulingWithId fromTrigger(Trigger trigger) {

        String jobId = trigger.getJobKey().getName();
        JobScheduling sched = new JobScheduling();
        sched.setStartAt(trigger.getStartTime().getTime());

        if (trigger instanceof CronTriggerImpl) {

            sched.setType(JobSchedulingType.CRON);

            CronTriggerImpl cronTrigger = (CronTriggerImpl) trigger;
            sched.setValue(cronTrigger.getCronExpression());

        }

        return new JobSchedulingWithId(jobId, sched);
    }

}
