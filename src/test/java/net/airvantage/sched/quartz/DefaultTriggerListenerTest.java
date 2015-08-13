package net.airvantage.sched.quartz;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.model.JobLock;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.services.JobStateService;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Trigger;

public class DefaultTriggerListenerTest {

    private DefaultTriggerListener service;

    @Mock
    private JobStateService jobStateService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        service = new DefaultTriggerListener(jobStateService);
    }

    @Test
    public void vetoJobExecution_withNoLock() throws AppException {

        // INPUT

        String jobId = "jobid";

        // MOCK

        JobKey key = new JobKey(jobId);
        Trigger trigger = Mockito.mock(Trigger.class);
        JobDetail detail = Mockito.mock(JobDetail.class);
        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);

        Mockito.when(context.getJobDetail()).thenReturn(detail);
        Mockito.when(detail.getKey()).thenReturn(key);

        JobState jobState = Mockito.mock(JobState.class);
        JobLock jobLock = Mockito.mock(JobLock.class);

        Mockito.when(jobStateService.find(Mockito.eq(jobId))).thenReturn(jobState);
        Mockito.when(jobState.getLock()).thenReturn(jobLock);

        // RUN

        Assert.assertFalse(service.vetoJobExecution(trigger, context));
    }

    @Test
    public void vetoJobExecution_withLock() throws AppException {

        // INPUT

        String jobId = "jobid";

        // MOCK

        JobKey key = new JobKey(jobId);
        Trigger trigger = Mockito.mock(Trigger.class);
        JobDetail detail = Mockito.mock(JobDetail.class);
        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);

        Mockito.when(context.getJobDetail()).thenReturn(detail);
        Mockito.when(detail.getKey()).thenReturn(key);

        JobState jobState = Mockito.mock(JobState.class);
        JobLock jobLock = Mockito.mock(JobLock.class);

        Mockito.when(jobStateService.find(Mockito.eq(jobId))).thenReturn(jobState);
        Mockito.when(jobState.getLock()).thenReturn(jobLock);
        Mockito.when(jobLock.isLocked()).thenReturn(true);

        // RUN

        Assert.assertTrue(service.vetoJobExecution(trigger, context));
    }

    @Test
    public void vetoJobExecution_withExpiredLock() throws AppException {

        // INPUT

        String jobId = "jobid";

        // MOCK

        JobKey key = new JobKey(jobId);
        Trigger trigger = Mockito.mock(Trigger.class);
        JobDetail detail = Mockito.mock(JobDetail.class);
        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);

        Mockito.when(context.getJobDetail()).thenReturn(detail);
        Mockito.when(detail.getKey()).thenReturn(key);

        JobState jobState = Mockito.mock(JobState.class);
        JobLock jobLock = Mockito.mock(JobLock.class);

        Mockito.when(jobStateService.find(Mockito.eq(jobId))).thenReturn(jobState);
        Mockito.when(jobState.getLock()).thenReturn(jobLock);
        Mockito.when(jobLock.isLocked()).thenReturn(true);
        Mockito.when(jobLock.isExpired()).thenReturn(true);

        // RUN

        Assert.assertFalse(service.vetoJobExecution(trigger, context));
    }

}
