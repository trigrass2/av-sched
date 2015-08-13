package net.airvantage.sched.model;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobLock {

    private static final Logger LOG = LoggerFactory.getLogger(JobLock.class);

    private boolean locked;
    private Long expiresAt;

    public JobLock() {
        locked = false;
        expiresAt = null;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    public Long getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Long expiresAt) {
        this.expiresAt = expiresAt;
    }

    public boolean isExpired() {
        return isExpired(new Date());
    }

    public boolean isExpired(Date date) {
        LOG.debug("Checking if expired : " + this.expiresAt + " vs " + date.getTime());
        boolean expired = (this.expiresAt != null && this.expiresAt < date.getTime());
        LOG.debug("Is it expired ? " + expired);
        return expired;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((expiresAt == null) ? 0 : expiresAt.hashCode());
        result = prime * result + (locked ? 1231 : 1237);
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
        JobLock other = (JobLock) obj;
        if (expiresAt == null) {
            if (other.expiresAt != null)
                return false;
        } else if (!expiresAt.equals(other.expiresAt))
            return false;
        if (locked != other.locked)
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("JobLock [locked=");
        builder.append(locked);
        builder.append(", expiresAt=");
        builder.append(expiresAt);
        builder.append("]");
        return builder.toString();
    }

}
