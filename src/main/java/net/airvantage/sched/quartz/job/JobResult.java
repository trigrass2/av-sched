package net.airvantage.sched.quartz.job;

public class JobResult {

    public enum CallbackStatus {
        SUCCESS, FAILURE
    };

    private CallbackStatus status;
    private String jobId;
    private boolean ack;
    private long retry; // this is a DELAY (not a timestamp)

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public CallbackStatus getStatus() {
        return status;
    }

    public void setStatus(CallbackStatus status) {
        this.status = status;
    }

    public boolean isAck() {
        return ack;
    }

    public void setAck(boolean ack) {
        this.ack = ack;
    }

    public long getRetry() {
        return retry;
    }

    public void setRetry(long retry) {
        this.retry = retry;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("JobResult [status=");
        builder.append(status);
        builder.append(", jobId=");
        builder.append(jobId);
        builder.append(", ack=");
        builder.append(ack);
        builder.append(", retry=");
        builder.append(retry);
        builder.append("]");
        return builder.toString();
    }

}
