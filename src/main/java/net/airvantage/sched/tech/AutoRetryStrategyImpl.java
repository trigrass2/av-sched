package net.airvantage.sched.tech;

import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.http.HttpResponse;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.protocol.HttpContext;

/**
 * Implementation of the {@link ServiceUnavailableRetryStrategy} interface that retries service unavailable responses
 * for a fixed number of times at a fixed interval.
 */
public class AutoRetryStrategyImpl implements ServiceUnavailableRetryStrategy {

    /**
     * Maximum number of allowed retries.
     */
    private final int maxRetries;

    /**
     * Retry interval between subsequent requests, in milliseconds.
     */
    private final long retryInterval;

    /**
     * HTTP code to check.
     */
    private final Set<Integer> httpCodes;

    public AutoRetryStrategyImpl(int maxRetries, int retryIntervalMs, Set<Integer> codes) {

        Validate.isTrue(maxRetries > 0, "Invalid max retries value");
        Validate.isTrue(retryIntervalMs > 0, "Invalid retry interval value");
        Validate.notEmpty(codes);

        this.maxRetries = maxRetries;
        this.retryInterval = retryIntervalMs;
        this.httpCodes = codes;
    }

    public boolean retryRequest(final HttpResponse response, final int executionCount, final HttpContext context) {
        return executionCount <= maxRetries && httpCodes.contains(response.getStatusLine().getStatusCode());
    }

    public long getRetryInterval() {
        return retryInterval;
    }

}
