package net.airvantage.sched.services;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import net.airvantage.sched.TestUtils;
import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.dao.JobWakeupDao;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.model.JobWakeup;
import net.airvantage.sched.quartz.job.JobResult;
import net.airvantage.sched.quartz.job.JobResult.CallbackStatus;
import net.airvantage.sched.services.tech.RetryPolicyHelper;

public class RetryPolicyServiceImplTest {

    private RetryPolicyHelper service;

    @Mock
    private JobWakeupDao jobWakeupDao;

    @Mock
    private JobStateService jobStateService;

    @Mock
    private JobSchedulingService jobSchedulingService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        service = new RetryPolicyHelper(jobStateService, jobSchedulingService, jobWakeupDao);
    }

    @Test
    public void jobExecuted_cronJobSuccess() throws AppException {

        // INPUT

        String jobId = "jobid";
        JobState state = TestUtils.cronJobState(jobId);

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);

        Mockito.when(result.getStatus()).thenReturn(CallbackStatus.SUCCESS);
        Mockito.when(result.getJobId()).thenReturn(jobId);
        Mockito.when(result.isAck()).thenReturn(true);
        Mockito.when(result.getRetry()).thenReturn(0l);

        Mockito.when(jobStateService.find(jobId)).thenReturn(state);

        // RUN

        service.handleResult(result);

        // VERIFY

        Mockito.verify(jobSchedulingService).ackJob(jobId);
    }

    @Test
    public void jobExecuted_wakeupJobSuccess() throws AppException {

        // INPUT

        String jobId = "job.id";
        String callback = "callback.url";

        JobWakeup wakeup = new JobWakeup();
        wakeup.setId(jobId);
        wakeup.setWakeupTime(123l);
        wakeup.setCallback(callback);

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);

        Mockito.when(result.getStatus()).thenReturn(CallbackStatus.SUCCESS);
        Mockito.when(result.getJobId()).thenReturn(jobId);
        Mockito.when(result.isAck()).thenReturn(true);
        Mockito.when(result.getRetry()).thenReturn(0l);

        // RUN

        service.handleResult(wakeup, result);

        // VERIFY

        Mockito.verify(jobWakeupDao).delete(jobId);
    }

    @Test
    public void jobExecuted_wakeupJobRetry() throws AppException {

        // INPUT

        String jobId = "job.id";
        String callback = "callback.url";

        JobWakeup wakeup = new JobWakeup();
        wakeup.setId(jobId);
        wakeup.setWakeupTime(123l);
        wakeup.setCallback(callback);

        long now = System.currentTimeMillis();
        long delay = 30_000L;

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);

        Mockito.when(result.getStatus()).thenReturn(CallbackStatus.SUCCESS);
        Mockito.when(result.getJobId()).thenReturn(jobId);
        Mockito.when(result.isAck()).thenReturn(false);
        Mockito.when(result.getRetry()).thenReturn(delay);

        // RUN

        service.handleResult(wakeup, result);

        // VERIFY

        ArgumentCaptor<JobWakeup> captor = ArgumentCaptor.forClass(JobWakeup.class);
        Mockito.verify(jobWakeupDao).persist(captor.capture());

        JobWakeup actual = captor.getValue();
        assertNotNull(actual.getWakeupTime());
        assertEquals((((now + delay) / 1000) * 1000), actual.getWakeupTime().longValue());
    }

    @Test
    public void jobExecuted_wakeupJobRetryDate() throws AppException {

        // INPUT

        String jobId = "job.id";
        String callback = "callback.url";

        JobWakeup wakeup = new JobWakeup();
        wakeup.setId(jobId);
        wakeup.setWakeupTime(123l);
        wakeup.setCallback(callback);

        long now = System.currentTimeMillis();
        long retryDate = now + 30_000L;

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);

        Mockito.when(result.getStatus()).thenReturn(CallbackStatus.SUCCESS);
        Mockito.when(result.getJobId()).thenReturn(jobId);
        Mockito.when(result.isAck()).thenReturn(false);
        Mockito.when(result.getRetryDate()).thenReturn(retryDate);

        // RUN

        service.handleResult(wakeup, result);

        // VERIFY

        ArgumentCaptor<JobWakeup> captor = ArgumentCaptor.forClass(JobWakeup.class);
        Mockito.verify(jobWakeupDao).persist(captor.capture());

        JobWakeup actual = captor.getValue();
        assertNotNull(actual.getWakeupTime());
        assertEquals(retryDate, actual.getWakeupTime().longValue());
    }

    @Test
    public void jobExecuted_wakeupJobManyRetry() throws AppException {

        // INPUT

        String jobId = "job.id";
        String callback = "callback.url";

        JobWakeup wakeup = new JobWakeup();
        wakeup.setId(jobId);
        wakeup.setWakeupTime(123l);
        wakeup.setCallback(callback);

        long now = System.currentTimeMillis();
        long delay = 30_000l;

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);

        Mockito.when(result.getStatus()).thenReturn(CallbackStatus.SUCCESS);
        Mockito.when(result.getJobId()).thenReturn(jobId);
        Mockito.when(result.isAck()).thenReturn(false);
        Mockito.when(result.getRetry()).thenReturn(delay);

        // RUN

        service.handleResult(wakeup, result);
        service.handleResult(wakeup, result); // 1 x retry
        service.handleResult(wakeup, result); // 2 x retry
        service.handleResult(wakeup, result); // 3 x retry
        service.handleResult(wakeup, result); // 4 x retry

        // VERIFY

        ArgumentCaptor<JobWakeup> captor = ArgumentCaptor.forClass(JobWakeup.class);
        Mockito.verify(jobWakeupDao, Mockito.times(5)).persist(captor.capture());

        JobWakeup actual = captor.getValue();
        long wakeupTime = now + 4000L;
        Assert.assertTrue(wakeupTime <= actual.getWakeupTime());
    }

    @Test
    public void triggerComplete_cronJobFailed() throws AppException {

        // INPUT

        String jobId = "jobid";
        JobState state = TestUtils.cronJobState(jobId);

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);

        Mockito.when(result.getStatus()).thenReturn(CallbackStatus.FAILURE);
        Mockito.when(result.getJobId()).thenReturn(jobId);
        Mockito.when(result.isAck()).thenReturn(false);
        Mockito.when(result.getRetry()).thenReturn(0l);

        Mockito.when(jobStateService.find(jobId)).thenReturn(state);

        // RUN

        service.handleResult(result);

        // VERIFY

        Mockito.verifyNoMoreInteractions(jobSchedulingService);
    }

    @Test
    public void triggerComplete_wakeupJobFailed() throws AppException {

        // INPUT

        String jobId = "job.id";
        String callback = "callback.url";

        JobWakeup wakeup = new JobWakeup();
        wakeup.setId(jobId);
        wakeup.setWakeupTime(123l);
        wakeup.setCallback(callback);

        long now = System.currentTimeMillis();

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);

        Mockito.when(result.getStatus()).thenReturn(CallbackStatus.FAILURE);
        Mockito.when(result.getJobId()).thenReturn(jobId);
        Mockito.when(result.isAck()).thenReturn(false);
        Mockito.when(result.getRetry()).thenReturn(0l);

        // RUN

        service.handleResult(wakeup, result);

        // VERIFY

        ArgumentCaptor<JobWakeup> captor = ArgumentCaptor.forClass(JobWakeup.class);
        Mockito.verify(jobWakeupDao).persist(captor.capture());

        JobWakeup actual = captor.getValue();
        assertNotNull(actual.getWakeupTime());
        assertEquals((((now + 1000L) / 1000) * 1000), actual.getWakeupTime().longValue());
    }

}
