package net.airvantage.sched.model;

import org.quartz.Job;

/**
 * The scheduling configuration of a {@link Job}.
 */
public class JobScheduling {

    private JobSchedulingType type;
    private long startAt;
    private String value;

    public JobSchedulingType getType() {
        return type;
    }

    public void setType(JobSchedulingType type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getStartAt() {
        return startAt;
    }

    public void setStartAt(long startAt) {
        this.startAt = startAt;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("JobScheduling [type=");
        builder.append(type);
        builder.append(", startAt=");
        builder.append(startAt);
        builder.append(", value=");
        builder.append(value);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (startAt ^ (startAt >>> 32));
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
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
        JobScheduling other = (JobScheduling) obj;
        if (startAt != other.startAt)
            return false;
        if (type != other.type)
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

}
