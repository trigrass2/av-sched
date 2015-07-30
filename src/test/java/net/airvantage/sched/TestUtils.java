package net.airvantage.sched;

import net.airvantage.sched.model.JobConfig;
import net.airvantage.sched.model.JobDef;
import net.airvantage.sched.model.JobLock;
import net.airvantage.sched.model.JobScheduling;
import net.airvantage.sched.model.JobSchedulingType;
import net.airvantage.sched.model.JobState;

public class TestUtils {

    public static JobState cronJobState(String id) {

        JobConfig config = new JobConfig();
        config.setId(id);
        config.setUrl("http://test/api/test");
        config.setTimeout(5000);

        JobState jobState = new JobState();
        jobState.setConfig(config);
        jobState.setLock(new JobLock());
        jobState.setScheduling(cronJobSchedulingDef("cron"));

        return jobState;
    }

    public static JobDef cronJobDef(String id, String cron) {

        JobConfig config = new JobConfig();
        config.setId(id);
        config.setUrl("http://test/api/test");
        config.setTimeout(5000);

        JobDef jobDef = new JobDef();
        jobDef.setConfig(config);
        jobDef.setScheduling(cronJobSchedulingDef(cron));

        return jobDef;
    }

    public static JobDef wakeupJobDef(String id, long date) {

        JobConfig config = new JobConfig();
        config.setId(id);
        config.setUrl("http://test/api/test");
        config.setTimeout(5000);

        JobDef jobDef = new JobDef();
        jobDef.setConfig(config);
        jobDef.setScheduling(wakeupJobSchedulingDef(date));

        return jobDef;
    }

    public static JobScheduling cronJobSchedulingDef(String cron) {

        JobScheduling schedDef = new JobScheduling();
        schedDef.setType(JobSchedulingType.CRON);
        schedDef.setValue(cron);

        return schedDef;
    }

    public static JobScheduling wakeupJobSchedulingDef(long date) {

        JobScheduling schedDef = new JobScheduling();
        schedDef.setType(JobSchedulingType.WAKEUP);
        schedDef.setValue(Long.toString(date));

        return schedDef;
    }

}
