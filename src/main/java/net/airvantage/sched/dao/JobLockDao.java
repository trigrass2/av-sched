package net.airvantage.sched.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import net.airvantage.sched.model.JobLock;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

/**
 * DAO to manage the {@link JobLock} object model.
 */
public class JobLockDao {

    private QueryRunner queryRunner;

    public JobLockDao(DataSource dataSource) {
        this.queryRunner = new QueryRunner(dataSource);
    }

    /**
     * Persist a new lock built from the given properties.
     */
    public void add(String id, long expiresAt) throws SQLException {
        Timestamp ts = new Timestamp(expiresAt);
        queryRunner.update("insert into sched_job_locks(id, expires_at) values(?,?)", id, ts);
    }

    /**
     * Delete the lock identified by the given identifier.
     */
    public void delete(String id) throws SQLException {
        queryRunner.update("delete from sched_job_locks where id=?", id);
    }

    /**
     * Return the lock identified by the given identifier.
     */
    public JobLock find(String id) throws SQLException {

        ResultSetHandler<JobLock> rsh = new ResultSetHandler<JobLock>() {
            @Override
            public JobLock handle(ResultSet rs) throws SQLException {

                if (!rs.next()) {
                    return new JobLock();

                } else {
                    JobLock lock = new JobLock();
                    lock.setLocked(true);
                    Timestamp expiresAt = rs.getTimestamp(2);
                    lock.setExpiresAt(expiresAt.getTime());
                    return lock;
                }
            }
        };

        return queryRunner.query("select id,expires_at from sched_job_locks where id=?", rsh, id);
    }

    /**
     * Return all the existing locks grouped by their identifier.
     */
    public Map<String, JobLock> findAll() throws SQLException {

        ResultSetHandler<Map<String, JobLock>> rsh = new ResultSetHandler<Map<String, JobLock>>() {
            @Override
            public Map<String, JobLock> handle(ResultSet rs) throws SQLException {

                Map<String, JobLock> map = new HashMap<String, JobLock>();

                while (rs.next()) {
                    JobLock lock = new JobLock();
                    lock.setLocked(true);
                    String id = rs.getString(1);
                    Timestamp expiresAt = rs.getTimestamp(2);
                    lock.setExpiresAt(expiresAt.getTime());
                    map.put(id, lock);
                }

                return map;
            }
        };
        return queryRunner.query("select id, expires_at from sched_job_locks", rsh);
    }

    /**
     * Delete all the existing locks.
     */
    public void deleteAll() throws SQLException {
        queryRunner.update("delete from sched_job_locks");
    }

}
