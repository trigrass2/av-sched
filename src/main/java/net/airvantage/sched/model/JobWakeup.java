package net.airvantage.sched.model;

/**
 * A wake-up job definition.
 */
public class JobWakeup {

    private String id;
    private Long wakeupTime;
    private String callback;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getWakeupTime() {
        return wakeupTime;
    }

    public void setWakeupTime(Long wakeupTime) {
        this.wakeupTime = wakeupTime;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((callback == null) ? 0 : callback.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((wakeupTime == null) ? 0 : wakeupTime.hashCode());
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
        JobWakeup other = (JobWakeup) obj;
        if (callback == null) {
            if (other.callback != null)
                return false;
        } else if (!callback.equals(other.callback))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (wakeupTime == null) {
            if (other.wakeupTime != null)
                return false;
        } else if (!wakeupTime.equals(other.wakeupTime))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("JobWakeup [id=");
        builder.append(id);
        builder.append(", wakeupTime=");
        builder.append(wakeupTime);
        builder.append(", callback=");
        builder.append(callback);
        builder.append("]");
        return builder.toString();
    }

}
