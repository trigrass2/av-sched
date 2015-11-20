package net.airvantage.sched.services.tech;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

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
    private RemoteServiceConnector connector;

    @Mock
    private JobConfigDao jobConfigDao;

    @Mock
    private JsonMapper jsonMapper;

    private String schedSecret = "secret";

    @Before
    public void setUp() {

        MockitoAnnotations.initMocks(this);
        service = new JobExecutionHelper(jobStateService, connector, schedSecret, jsonMapper, jobConfigDao,
                retryPolicyHelper);
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testExecute_cronSuccess() throws Exception {

        // INPUT

        String jobId = "job.id";
        String callback = "http://callback.service.url";
        URI url = new URI(callback);

        PostHttpJobResult callbackResult = new PostHttpJobResult();
        callbackResult.setAck(true);
        callbackResult.setRetry(33l);

        // MOCK

        JobConfig config = Mockito.mock(JobConfig.class);

        Mockito.when(jobConfigDao.find(Mockito.eq(jobId))).thenReturn(config);
        Mockito.when(config.getUrl()).thenReturn(callback);

        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(connector.post(Mockito.eq(url), Mockito.anyMap())).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        Mockito.when(httpEntity.getContent()).thenReturn(Mockito.mock(InputStream.class));

        Mockito.when(jsonMapper.postHttpJobResult(Mockito.any(InputStream.class))).thenReturn(callbackResult);

        // RUN

        JobResult result = service.execute(jobId);

        // VERIFY

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connector).post(Mockito.eq(url), captor.capture());
        Map<String, String> headers = captor.getValue();
        Assert.assertEquals(schedSecret, headers.get("X-Sched-secret"));

        Assert.assertEquals(JobResult.CallbackStatus.SUCCESS, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertEquals(callbackResult.getAck(), result.isAck());
        Assert.assertEquals(callbackResult.getRetry().longValue(), result.getRetry());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testExecute_wakeupSuccess() throws Exception {

        // INPUT

        String jobId = "job.id";
        String callback = "http://callback.service.url";
        URI url = new URI(callback);

        JobWakeup wakeup = new JobWakeup();
        wakeup.setWakeupTime(System.currentTimeMillis());
        wakeup.setCallback(callback);
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

        Mockito.when(connector.post(Mockito.eq(url), Mockito.anyMap())).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        Mockito.when(httpEntity.getContent()).thenReturn(Mockito.mock(InputStream.class));

        Mockito.when(jsonMapper.postHttpJobResult(Mockito.any(InputStream.class))).thenReturn(callbackResult);

        // RUN

        JobResult result = service.execute(wakeup);

        // VERIFY

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connector).post(Mockito.eq(url), captor.capture());
        Map<String, String> headers = captor.getValue();
        Assert.assertEquals(schedSecret, headers.get("X-Sched-secret"));

        Mockito.verify(retryPolicyHelper).handleResult(Mockito.eq(wakeup), Mockito.eq(result));

        Assert.assertEquals(JobResult.CallbackStatus.SUCCESS, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertEquals(callbackResult.getAck(), result.isAck());
        Assert.assertEquals(callbackResult.getRetry().longValue(), result.getRetry());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testExecute_cronFailure() throws Exception {

        // INPUT

        String jobId = "job.id";
        String callback = "http://callback.service.url";
        URI url = new URI(callback);

        // MOCK

        JobConfig config = Mockito.mock(JobConfig.class);

        Mockito.when(jobConfigDao.find(Mockito.eq(jobId))).thenReturn(config);
        Mockito.when(config.getUrl()).thenReturn(callback);

        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        HttpEntity httpEntity = Mockito.mock(HttpEntity.class);

        Mockito.when(connector.post(Mockito.eq(url), Mockito.anyMap())).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);

        // RUN

        JobResult result = service.execute(jobId);

        // VERIFY

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connector).post(Mockito.eq(url), captor.capture());
        Map<String, String> headers = captor.getValue();
        Assert.assertEquals(schedSecret, headers.get("X-Sched-secret"));

        Assert.assertEquals(JobResult.CallbackStatus.FAILURE, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertFalse(result.isAck());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testExecute_wakeupFailure() throws Exception {

        // INPUT

        String jobId = "job.id";
        String callback = "http://callback.service.url";
        URI url = new URI(callback);

        JobWakeup wakeup = new JobWakeup();
        wakeup.setWakeupTime(System.currentTimeMillis());
        wakeup.setCallback(callback);
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

        Mockito.when(connector.post(Mockito.eq(url), Mockito.anyMap())).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);

        // RUN

        JobResult result = service.execute(wakeup);

        // VERIFY

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connector).post(Mockito.eq(url), captor.capture());
        Map<String, String> headers = captor.getValue();
        Assert.assertEquals(schedSecret, headers.get("X-Sched-secret"));

        Mockito.verify(retryPolicyHelper).handleResult(Mockito.eq(wakeup), Mockito.eq(result));

        Assert.assertEquals(JobResult.CallbackStatus.FAILURE, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertFalse(result.isAck());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testExecute_connRefuse() throws Exception {

        // INPUT

        String jobId = "job.id";
        String callback = "http://callback.service.url";
        URI url = new URI(callback);

        // MOCK

        JobConfig config = Mockito.mock(JobConfig.class);

        Mockito.when(jobConfigDao.find(Mockito.eq(jobId))).thenReturn(config);
        Mockito.when(config.getUrl()).thenReturn(callback);

        Mockito.when(connector.post(Mockito.eq(url), Mockito.anyMap())).thenThrow(new IOException("connection refuse"));

        // RUN

        JobResult result = service.execute(jobId);

        // VERIFY

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connector).post(Mockito.eq(url), captor.capture());
        Map<String, String> headers = captor.getValue();
        Assert.assertEquals(schedSecret, headers.get("X-Sched-secret"));

        Assert.assertEquals(JobResult.CallbackStatus.FAILURE, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertFalse(result.isAck());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testExecute_noResponse() throws Exception {

        // INPUT

        String jobId = "job.id";
        String callback = "http://callback.service.url";
        URI url = new URI(callback);

        // MOCK

        JobConfig config = Mockito.mock(JobConfig.class);

        Mockito.when(jobConfigDao.find(Mockito.eq(jobId))).thenReturn(config);
        Mockito.when(config.getUrl()).thenReturn(callback);

        Mockito.when(connector.post(Mockito.eq(url), Mockito.anyMap())).thenReturn(null);

        // RUN

        JobResult result = service.execute(jobId);

        // VERIFY

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(connector).post(Mockito.eq(url), captor.capture());
        Map<String, String> headers = captor.getValue();
        Assert.assertEquals(schedSecret, headers.get("X-Sched-secret"));

        Assert.assertEquals(JobResult.CallbackStatus.FAILURE, result.getStatus());
        Assert.assertEquals(jobId, result.getJobId());
        Assert.assertFalse(result.isAck());
    }

}
