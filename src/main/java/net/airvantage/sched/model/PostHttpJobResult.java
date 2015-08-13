package net.airvantage.sched.model;

/**
 * A bean to deserialize JSON content returned by callback.
 */
public class PostHttpJobResult {

    private Boolean ack;
    private Long retry;

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

}
