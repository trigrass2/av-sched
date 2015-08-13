package net.airvantage.sched.quartz;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.quartz.job.JobResult;
import net.airvantage.sched.services.tech.RetryPolicyHelper;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.quartz.JobExecutionContext;

public class DefaultJobListenerTest {

    private DefaultJobListener service;

    @Mock
    private RetryPolicyHelper retryPolicyHelper;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        service = new DefaultJobListener(retryPolicyHelper);
    }

    @Test
    public void jobWasExecuted_noResult() {

        // MOCK

        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);
        Mockito.when(context.getResult()).thenReturn(null);

        // RUN

        service.jobWasExecuted(context, null);

        // VERIFY

        Mockito.verifyNoMoreInteractions(retryPolicyHelper);
    }

    @Test
    public void jobWasExecuted_nominal() throws AppException {

        // MOCK

        JobResult result = Mockito.mock(JobResult.class);
        JobExecutionContext context = Mockito.mock(JobExecutionContext.class);

        Mockito.when(context.getResult()).thenReturn(result);

        // RUN

        service.jobWasExecuted(context, null);

        // VERIFY

        Mockito.verify(retryPolicyHelper).handleResult(Mockito.eq(result));
    }

}
