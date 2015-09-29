package net.airvantage.sched.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.airvantage.sched.app.exceptions.DaoRuntimeException;
import net.airvantage.sched.model.JobWakeup;

/**
 * DAO to manage the {@link JobWakeup} object model.
 */
public class JobWakeupDao {

    private final static Logger LOG = LoggerFactory.getLogger(JobWakeupDao.class);

    private QueryExecutor queryExecutor;

    public JobWakeupDao(DataSource dataSource) throws DaoRuntimeException {
        this.queryExecutor = new QueryExecutor(dataSource);
    }

    /**
     * Persists the given wake-up and update it if already exists (same id).
     */
    public void persist(JobWakeup wakeup) throws DaoRuntimeException {
        LOG.debug("persist : wakeup={}", wakeup);

        // If the wakeup is in the past, it will be woken up immediately
        Long wakeupTime = wakeup.getWakeupTime();
        if (wakeupTime == null) {
            wakeup.setWakeupTime(System.currentTimeMillis());
        }

        try {
            queryExecutor.update(
                    "insert into sched_job_wakeups(id,wakeup_time,callback,retry_count) values(?,?,?,?) on duplicate key update wakeup_time=?, callback=?, retry_count=?",
                    wakeup.getId(), wakeup.getWakeupTime(), wakeup.getCallback(), wakeup.getRetryCount(),
                    wakeup.getWakeupTime(), wakeup.getCallback(), wakeup.getRetryCount());

        } catch (SQLException ex) {
            throw new DaoRuntimeException(ex);
        }
    }

    /**
     * Deletes the wake-up with the given identifier.
     */
    public void delete(String wakeupId) throws DaoRuntimeException {
        LOG.debug("delete : wakeupId={}", wakeupId);

        try {
            queryExecutor.update("delete from sched_job_wakeups where id=?", wakeupId);

        } catch (SQLException ex) {
            throw new DaoRuntimeException(ex);
        }
    }

    /**
     * Delete all the existing wake-ups.
     */
    public void deleteAll() throws DaoRuntimeException {

        try {
            queryExecutor.update("delete from sched_job_wakeups");

        } catch (SQLException ex) {
            throw new DaoRuntimeException(ex);
        }
    }

    /**
     * Returns the {@code limit} first wake-ups with a scheduled date before the specified date.
     */
    public List<JobWakeup> find(long to, int limit) throws DaoRuntimeException {
        LOG.debug("find : to={}, limit={}", to, limit);

        ResultSetHandler<List<JobWakeup>> rsh = (ResultSet rs) -> {

            List<JobWakeup> res = new ArrayList<>();

            while (rs.next()) {
                JobWakeup wakeup = new JobWakeup();
                wakeup.setId(rs.getString(1));
                wakeup.setWakeupTime(rs.getLong(2));
                wakeup.setCallback(rs.getString(3));
                wakeup.setRetryCount(rs.getInt(4));

                res.add(wakeup);
            }

            return res;
        };

        try {
            return queryExecutor.query(
                    "select id, wakeup_time, callback, retry_count from sched_job_wakeups where wakeup_time < ? LIMIT ?",
                    rsh, to, limit);
        } catch (SQLException ex) {
            throw new DaoRuntimeException(ex);
        }
    }
}
