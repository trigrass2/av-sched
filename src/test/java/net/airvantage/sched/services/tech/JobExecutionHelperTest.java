package net.airvantage.sched.services.tech;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import net.airvantage.sched.app.mapper.JsonMapper;
import net.airvantage.sched.dao.JobConfigDao;
import net.airvantage.sched.model.JobConfig;
import net.airvantage.sched.model.JobWakeup;
import net.airvantage.sched.model.PostHttpJobResult;
import net.airvantage.sched.quartz.job.JobResult;
import net.airvantage.sched.services.JobStateService;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;

public class JobExecutionHelperTest {

    private JobExecutionHelper service;

    @Mock
    private RetryPolicyHelper retryPolicyHelper;

    @Mock
    private JobStateService jobStateService;

    @Mock
    private CloseableHttpClient client;

    @Mock
    private JobConfigDao jobConfigDao;

    @Mock
    private JsonMapper jsonMapper;

    private String schedSecret = "secret";

    @Before
    public void setUp() {

        MockitoAnnotations.initMocks(this);
        service = new JobExecutionHelper(jobStateService, client, schedSecret, jsonMapper, jobConfigDao,
                retryPolicyHelper);
    }

    @Test
    public void testExecute_cronSuccess() throws Exception {

        // INPUT

        String jobId = "job.id";
        String url = "job.url";

        PostHttpJobResult callbackResult = new PostHttpJobResult();
        callbackResult.setAck(true);
        callbackResult.setRetry(33l);

        // MOCK

        JobConfig config = Mockito.mock(JobConfig.class);

        Mockito.when(jobConfigDao.find(Mockito.eq(jobId))).thenReturn(config);
        Mockito.when(config.getUrl()).thenReturn(url);

        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(client.execute(Mockito.any(HttpPost.class))).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        Mockito.when(httpEntity.getContent()).thenReturn(Mockito.mock(InputStream.class));

        Mockito.when(jsonMapper.postHttpJobResult(Mockito.any(InputStream.class))).thenReturn(callbackResult);

        // RUN

        JobResult result = service.execute(jobId);

        // VERIFY

        ArgumentCaptor<HttpPost> captor = ArgumentCaptor.forClass(HttpPost.class);
        Mockito.verify(client).execute(captor.capture());
        List<HttpPost> posts = captor.getAllValues();
        HttpPost post = posts.get(0);
        Assert.assertEquals(schedSecret, post.getHeaders("X-Sched-Secret")[0].getValue());

        Assert.assertEquals(JobResult.CallbackStatus.SUCCESS, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertEquals(callbackResult.getAck(), result.isAck());
        Assert.assertEquals(callbackResult.getRetry().longValue(), result.getRetry());
    }

    @Test
    public void testExecute_wakeupSuccess() throws Exception {

        // INPUT

        String jobId = "job.id";
        String url = "job.url";

        JobWakeup wakeup = new JobWakeup();
        wakeup.setWakeupTime(System.currentTimeMillis());
        wakeup.setCallback(url);
        wakeup.setId(jobId);

        PostHttpJobResult callbackResult = new PostHttpJobResult();
        callbackResult.setAck(true);
        callbackResult.setRetry(123456789l);

        // MOCK

        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);
        JobDetail detail = Mockito.mock(JobDetail.class);
        JobKey key = new JobKey(jobId);
        JobDataMap datamap = Mockito.mock(JobDataMap.class);

        Mockito.when(context.getJobDetail()).thenReturn(detail);
        Mockito.when(detail.getKey()).thenReturn(key);
        Mockito.when(detail.getJobDataMap()).thenReturn(datamap);

        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(client.execute(Mockito.any(HttpPost.class))).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        Mockito.when(httpEntity.getContent()).thenReturn(Mockito.mock(InputStream.class));

        Mockito.when(jsonMapper.postHttpJobResult(Mockito.any(InputStream.class))).thenReturn(callbackResult);

        // RUN

        JobResult result = service.execute(wakeup);

        // VERIFY

        ArgumentCaptor<HttpPost> captor = ArgumentCaptor.forClass(HttpPost.class);
        Mockito.verify(client).execute(captor.capture());
        List<HttpPost> posts = captor.getAllValues();
        HttpPost post = posts.get(0);
        Assert.assertEquals(schedSecret, post.getHeaders("X-Sched-Secret")[0].getValue());

        Mockito.verify(retryPolicyHelper).handleResult(Mockito.eq(wakeup), Mockito.eq(result));

        Assert.assertEquals(JobResult.CallbackStatus.SUCCESS, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertEquals(callbackResult.getAck(), result.isAck());
        Assert.assertEquals(callbackResult.getRetry().longValue(), result.getRetry());
    }

    @Test
    public void testExecute_cronFailure() throws Exception {

        // INPUT

        String jobId = "job.id";
        String url = "job.url";

        // MOCK

        JobConfig config = Mockito.mock(JobConfig.class);

        Mockito.when(jobConfigDao.find(Mockito.eq(jobId))).thenReturn(config);
        Mockito.when(config.getUrl()).thenReturn(url);

        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(client.execute(Mockito.any(HttpPost.class))).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);

        // RUN

        JobResult result = service.execute(jobId);

        // VERIFY

        ArgumentCaptor<HttpPost> captor = ArgumentCaptor.forClass(HttpPost.class);
        Mockito.verify(client).execute(captor.capture());
        List<HttpPost> posts = captor.getAllValues();
        HttpPost post = posts.get(0);
        Assert.assertEquals(schedSecret, post.getHeaders("X-Sched-Secret")[0].getValue());

        Assert.assertEquals(JobResult.CallbackStatus.FAILURE, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertFalse(result.isAck());
    }

    @Test
    public void testExecute_wakeupFailure() throws Exception {

        // INPUT

        String jobId = "job.id";
        String url = "job.url";

        JobWakeup wakeup = new JobWakeup();
        wakeup.setWakeupTime(System.currentTimeMillis());
        wakeup.setCallback(url);
        wakeup.setId(jobId);

        // MOCK

        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);
        JobDetail detail = Mockito.mock(JobDetail.class);
        JobKey key = new JobKey(jobId);
        JobDataMap datamap = Mockito.mock(JobDataMap.class);

        Mockito.when(context.getJobDetail()).thenReturn(detail);
        Mockito.when(detail.getKey()).thenReturn(key);
        Mockito.when(detail.getJobDataMap()).thenReturn(datamap);

        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(client.execute(Mockito.any(HttpPost.class))).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);

        // RUN

        JobResult result = service.execute(wakeup);

        // VERIFY

        ArgumentCaptor<HttpPost> captor = ArgumentCaptor.forClass(HttpPost.class);
        Mockito.verify(client).execute(captor.capture());
        List<HttpPost> posts = captor.getAllValues();
        HttpPost post = posts.get(0);
        Assert.assertEquals(schedSecret, post.getHeaders("X-Sched-Secret")[0].getValue());

        Mockito.verify(retryPolicyHelper).handleResult(Mockito.eq(wakeup), Mockito.eq(result));

        Assert.assertEquals(JobResult.CallbackStatus.FAILURE, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertFalse(result.isAck());
    }

    @Test
    public void testExecute_connRefuse() throws Exception {

        // INPUT

        String jobId = "job.id";
        String url = "job.url";

        // MOCK

        JobConfig config = Mockito.mock(JobConfig.class);

        Mockito.when(jobConfigDao.find(Mockito.eq(jobId))).thenReturn(config);
        Mockito.when(config.getUrl()).thenReturn(url);

        Mockito.when(client.execute(Mockito.any(HttpPost.class))).thenThrow(new IOException("connection refuse"));

        // RUN

        JobResult result = service.execute(jobId);

        // VERIFY

        ArgumentCaptor<HttpPost> captor = ArgumentCaptor.forClass(HttpPost.class);
        Mockito.verify(client).execute(captor.capture());
        List<HttpPost> posts = captor.getAllValues();
        HttpPost post = posts.get(0);
        Assert.assertEquals(schedSecret, post.getHeaders("X-Sched-Secret")[0].getValue());

        Assert.assertEquals(JobResult.CallbackStatus.FAILURE, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertFalse(result.isAck());
    }

}
