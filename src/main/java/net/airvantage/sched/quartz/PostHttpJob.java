package net.airvantage.sched.quartz;

import java.io.IOException;

import net.airvantage.sched.app.ServiceLocator;
import net.airvantage.sched.dao.JobStateDao;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.model.JobState;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHttpRequest;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class PostHttpJob implements Job {

    private static final Logger LOG = Logger.getLogger(PostHttpJob.class);
    
    private HttpClient http;
    private JobStateDao jobStateDao;
    
    public PostHttpJob() {
        this(ServiceLocator.getHttpClient(),
                ServiceLocator.getJobStateDao());
    }
    
    protected PostHttpJob(HttpClient httpClient, JobStateDao jobStateDao) {
        this.http = httpClient;
        this.jobStateDao = jobStateDao;
    }
    
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String jobId = context.getJobDetail().getKey().getName();
        doJob(jobId);
    }

    protected void doJob(String jobId) throws JobExecutionException {
        JobState jobState = this.jobStateDao.findJobState(jobId);
        HttpPost request = new HttpPost(jobState.getUrl());
        try {
            HttpResponse response = this.http.execute(request);
            if (response.getStatusLine().getStatusCode() == 200) {
                jobStateDao.lockJob(jobId);
            } else {
                // TODO(pht) notify logical "logging" system
                LOG.error("TODO(pht) What do we want, again ?");
            }
        } catch (IOException e) {
            LOG.error(e);
            // TODO(pht) notify "logging" logical system
        }
    }
    
}
