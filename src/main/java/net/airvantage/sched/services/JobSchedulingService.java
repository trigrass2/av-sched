package net.airvantage.sched.services;

import org.quartz.Job;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.model.JobDef;
import net.airvantage.sched.model.JobScheduling;

/**
 * Service to manage {@link Job} scheduling.
 */
public interface JobSchedulingService {

    /**
     * Schedule a new job. A job with the same id will be replaced.
     */
    void scheduleJob(JobDef jobDef) throws AppException;

    /**
     * Re-schedule an existing job. A trigger with the same key should exist to be replaced.
     */
    void rescheduleJob(String jobId, JobScheduling conf) throws AppException;

    /**
     * Delete the scheduling configuration.
     */
    boolean unscheduleJob(String jobId) throws AppException;

    /**
     * Remove the lock of the given job if it exists. All the job configuration is deleted if the job is complete (no
     * scheduling configuration). A job is complete when the scheduling configuration is applied and finished.
     */
    void ackJob(String jobId) throws AppException;

    boolean triggerJob(String jobId) throws AppException;

    void clearJobs() throws AppException;

}
