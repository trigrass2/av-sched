package net.airvantage.sched.model;

public class JobState {

    private JobConfig config;
    private JobLock lock;
    private JobScheduling scheduling;

    public JobConfig getConfig() {
        return config;
    }

    public JobScheduling getScheduling() {
        return scheduling;
    }

    public void setScheduling(JobScheduling scheduling) {
        this.scheduling = scheduling;
    }

    public void setConfig(JobConfig config) {
        this.config = config;
    }

    public JobLock getLock() {
        return lock;
    }

    public void setLock(JobLock lock) {
        this.lock = lock;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((config == null) ? 0 : config.hashCode());
        result = prime * result + ((lock == null) ? 0 : lock.hashCode());
        result = prime * result + ((scheduling == null) ? 0 : scheduling.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JobState other = (JobState) obj;
        if (config == null) {
            if (other.config != null)
                return false;
        } else if (!config.equals(other.config))
            return false;
        if (lock == null) {
            if (other.lock != null)
                return false;
        } else if (!lock.equals(other.lock))
            return false;
        if (scheduling == null) {
            if (other.scheduling != null)
                return false;
        } else if (!scheduling.equals(other.scheduling))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("JobState [config=");
        builder.append(config);
        builder.append(", lock=");
        builder.append(lock);
        builder.append(", scheduling=");
        builder.append(scheduling);
        builder.append("]");
        return builder.toString();
    }

}
