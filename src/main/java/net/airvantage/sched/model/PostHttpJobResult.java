package net.airvantage.sched.model;

/**
 * A bean to deserialize JSON content returned by callback.
 */
public class PostHttpJobResult {

    private Boolean ack;
    private Long retry;
    private Long retryDate;

    public Boolean getAck() {
        return ack;
    }

    public void setAck(Boolean ack) {
        this.ack = ack;
    }

    public Long getRetry() {
        return retry;
    }

    public void setRetry(Long retry) {
        this.retry = retry;
    }

    public Long getRetryDate() {
        return retryDate;
    }

    public void setRetryDate(Long retryDate) {
        this.retryDate = retryDate;
    }

}
