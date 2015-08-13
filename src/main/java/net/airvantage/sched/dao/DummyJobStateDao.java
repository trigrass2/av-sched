package net.airvantage.sched.dao;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.model.JobDef;
import net.airvantage.sched.model.JobLock;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.services.JobStateService;

public class DummyJobStateDao implements JobStateService {

    private Map<String, JobState> states = new HashMap<String, JobState>();

    public void saveJobDef(JobDef jobDef) {
        JobState st = new JobState();
        st.setConfig(jobDef.getConfig());
        states.put(jobDef.getConfig().getId(), st);
    }

    public void deleteJobDef(String jobId) throws AppException {
        if (states.containsKey(jobId)) {
            states.remove(jobId);
        }
    }

    public JobState find(String id) {
        return states.get(id);
    }

    public void deleteAll() throws AppException {
        states.clear();
    }

    public void lockJob(String id) {

        // TODO(pht) move to a "lock" method ?
        JobState st = find(id);
        JobLock lock = new JobLock();
        lock.setLocked(true);
        lock.setExpiresAt(new Date().getTime() + st.getConfig().getTimeout());
        st.setLock(lock);

        states.put(id, st);
    }

    public void unlockJob(String id) {
        // TODO(pht) move to a "lock" method ?
        JobState st = find(id);
        JobLock lock = new JobLock();
        lock.setLocked(false);
        lock.setExpiresAt(null);
        st.setLock(lock);

        states.put(id, st);
    }

    public List<JobState> findAll() throws AppException {
        List<JobState> res = new ArrayList<JobState>();
        res.addAll(states.values());
        return res;
    }

}
