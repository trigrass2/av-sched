package net.airvantage.sched.services;

import java.util.List;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.model.JobState;

/**
 * A service to manage the jobs state
 */
public interface JobStateService {

    /**
     * Return the job state identified by the given identifier.
     */
    public abstract JobState find(String id) throws AppException;

    /**
     * Return all the existing job states.
     */
    public abstract List<JobState> findAll() throws AppException;

    /**
     * Add a new lock according to the current state.
     */
    public abstract void lockJob(String id) throws AppException;

    /**
     * Unlock an existing job.
     */
    public abstract void unlockJob(String id) throws AppException;

}