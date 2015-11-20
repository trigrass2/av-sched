package net.airvantage.sched.services.tech;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.airvantage.sched.app.SchedSecretFilter;
import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.app.mapper.JsonMapper;
import net.airvantage.sched.dao.JobConfigDao;
import net.airvantage.sched.model.JobConfig;
import net.airvantage.sched.model.JobWakeup;
import net.airvantage.sched.model.PostHttpJobResult;
import net.airvantage.sched.quartz.job.JobResult;
import net.airvantage.sched.quartz.job.JobResult.CallbackStatus;
import net.airvantage.sched.services.JobStateService;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobExecutionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionHelper.class);

    private RetryPolicyHelper retryPolicyHelper;
    private JobStateService jobStateService;
    private RemoteServiceConnector connector;
    private JobConfigDao jobConfigDao;
    private JsonMapper jsonMapper;
    private String schedSecret;

    // ------------------------------------------------- Constructors -------------------------------------------------

    public JobExecutionHelper(JobStateService jobStateService, RemoteServiceConnector connector, String schedSecret,
            JsonMapper jsonMapper, JobConfigDao jobConfigDao, RetryPolicyHelper retryPolicyHelper) {

        this.connector = connector;
        this.jsonMapper = jsonMapper;
        this.schedSecret = schedSecret;
        this.jobConfigDao = jobConfigDao;
        this.jobStateService = jobStateService;
        this.retryPolicyHelper = retryPolicyHelper;
    }

    // ------------------------------------------------- Public Methods -----------------------------------------------

    /**
     * Execute a CRON job.
     */
    public JobResult execute(String jobId) throws AppException {

        JobResult result = null;
        try {

            JobConfig config = this.jobConfigDao.find(jobId);
            if (config != null) {

                String url = config.getUrl();

                // Send a request to the job's callback
                result = this.doHttpPost(jobId, url);

                // Lock the job until acknowledgment
                if (result.getStatus() == CallbackStatus.SUCCESS) {
                    if (!result.isAck()) {
                        jobStateService.lockJob(jobId);
                    }
                }

                // Retry is managed asynchronously by the DefaultJobListener

            } else {
                LOG.warn("Try to executed a job {} without configuration", jobId);
            }

        } catch (Exception ex) {
            LOG.error("Unable to execute CRON job " + jobId, ex);
            throw new AppException("execute.job.error", Arrays.asList(jobId), ex);
        }

        return result;
    }

    /**
     * Execute a WAKEUP job.
     */
    public JobResult execute(JobWakeup wakeup) {

        JobResult result = null;
        if (wakeup != null) {

            String jobId = wakeup.getId();
            String url = wakeup.getCallback();

            // Send a request to the job's callback
            result = this.doHttpPost(jobId, url);

            // Handle retries and errors
            this.retryPolicyHelper.handleResult(wakeup, result);
        }

        return result;
    }

    // ------------------------------------------------- Private Methods ----------------------------------------------

    private JobResult doHttpPost(String jobId, String url) {
        LOG.debug("doHttpPost : jobId={}, url={}", jobId, url);

        JobResult result = null;
        try {

            Map<String, String> headers = new HashMap<>();
            headers.put(SchedSecretFilter.SCHED_SECRET_HEADER_NAME, schedSecret);

            CloseableHttpResponse response = this.connector.post(new URI(url), headers);
            if (response != null) {

                try {
                    if (response.getStatusLine().getStatusCode() == HttpURLConnection.HTTP_OK) {
                        result = this.requestSuccess(jobId, response.getEntity());

                    } else {
                        LOG.warn("Post to {} returns HTTP {}.", url, response.getStatusLine().getStatusCode());
                    }

                } finally {
                    response.close();
                }
            }

        } catch (Exception ex) {
            LOG.error("Unable to post to url  (job " + jobId + ")", ex);
        }

        if (result == null) {
            result = this.requestFailure(jobId);
        }

        return result;
    }

    private JobResult requestSuccess(String jobId, HttpEntity entity) {

        JobResult result = new JobResult();
        result.setStatus(CallbackStatus.SUCCESS);
        result.setJobId(jobId);

        try {
            if (entity != null) {
                InputStream stream = this.getStream(entity);

                if (stream != null) {
                    try {

                        PostHttpJobResult content = jsonMapper.postHttpJobResult(stream);
                        if (content != null) {

                            if (content.getAck() != null) {
                                result.setAck(content.getAck());
                            }
                            if (content.getRetry() != null) {
                                result.setRetry(content.getRetry());
                            }
                            if (content.getRetryDate() != null) {
                                result.setRetryDate(content.getRetryDate());
                            }
                        }

                    } finally {
                        stream.close();
                    }
                }
            }

        } catch (AppException | IllegalStateException | IOException e) {
            LOG.error("Invalid callback response (job " + jobId + "), it will be ignored.", e);
        }

        return result;
    }

    /**
     * Handle possible empty response content.
     */
    private InputStream getStream(HttpEntity entity) throws IOException {

        InputStream stream = entity.getContent();
        PushbackInputStream in = null;

        if (stream != null) {

            in = new PushbackInputStream(stream);
            int firstByte = in.read();

            if (firstByte != -1) {
                in.unread(firstByte);

            } else {
                in.close();
                in = null;
            }
        }

        return in;
    }

    private JobResult requestFailure(String jobId) {

        JobResult result = new JobResult();
        result.setStatus(CallbackStatus.FAILURE);
        result.setJobId(jobId);

        return result;
    }

}
