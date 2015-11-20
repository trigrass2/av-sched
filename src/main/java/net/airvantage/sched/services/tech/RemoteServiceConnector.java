package net.airvantage.sched.services.tech;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component to send HTTP requests. A retry logic is applied to manage possible connection errors.
 */
public class RemoteServiceConnector {

    private final static Logger LOG = LoggerFactory.getLogger(RemoteServiceConnector.class);

    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 60 * 1000;

    /**
     * The max number of retries done before to raise an error.
     */
    private final int maxRetries;

    private final CloseableHttpClient client;

    // ------------------------------------------------- Constructors -------------------------------------------------

    public RemoteServiceConnector(CloseableHttpClient client, int maxRetries) {

        this.client = client;
        this.maxRetries = maxRetries;
    }

    // ------------------------------------------------- Public Methods -----------------------------------------------

    /**
     * Send a POST HTTP request to the given service synchronously.
     */
    public CloseableHttpResponse post(URI service, Map<String, String> headers) throws IOException {

        int status = -1;
        CloseableHttpResponse response = null;

        int retries = 0;
        boolean retry = false;
        Exception ex = null;

        do {
            try {

                if (retries > 0) {
                    LOG.info("HTTP post retry {}/{}", retries, maxRetries);

                    // Wait before the next retry
                    try {
                        Thread.sleep(getWaitTime(retries - 1));

                    } catch (InterruptedException iex) {
                        LOG.warn("Retry interval sleep interrupted.", iex);
                    }
                }

                // Send the request
                HttpPost request = this.buildRequest(service, headers);
                response = this.client.execute(request);
                status = response.getStatusLine().getStatusCode();
                ex = null;

                // Handle the request result
                switch (status) {

                case HttpURLConnection.HTTP_BAD_GATEWAY:
                case HttpURLConnection.HTTP_UNAVAILABLE:
                case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
                    retry = true;
                    break;

                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_BAD_REQUEST:
                case HttpURLConnection.HTTP_FORBIDDEN:
                case HttpURLConnection.HTTP_INTERNAL_ERROR:
                default:
                    retry = false;
                    break;
                }

            } catch (IOException ioex) {
                LOG.warn(String.format("HTTP post error - %s", service.toASCIIString()), ioex);
                retry = true;
                ex = ioex;
            }

        } while (retry && (retries++ < maxRetries));

        // Log error message if the last request failed.
        if (status != HttpURLConnection.HTTP_OK) {
            String m = String.format("HTTP post to %s failed after %d retries, returned HTTP code %s",
                    service.toASCIIString(), retries - 1, status);

            if (ex != null) {
                LOG.error(m, ex);

            } else {
                LOG.error(m);
            }
        }

        return response;
    }

    // ------------------------------------------------ Private Methods -----------------------------------------------

    private HttpPost buildRequest(URI url, Map<String, String> headers) {

        HttpPost request = new HttpPost(url);
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                request.setHeader(header.getKey(), header.getValue());
            }
        }

        RequestConfig rqCfg = RequestConfig.custom().setConnectTimeout(DEFAULT_REQUEST_TIMEOUT_MS)
                .setConnectionRequestTimeout(DEFAULT_REQUEST_TIMEOUT_MS).build();
        request.setConfig(rqCfg);

        return request;
    }

    /**
     * Returns the next wait interval, in milliseconds, using an exponential backoff algorithm.
     */
    private long getWaitTime(int retryCount) {

        return (long) Math.pow(2, retryCount) * 100L;
    }

}
