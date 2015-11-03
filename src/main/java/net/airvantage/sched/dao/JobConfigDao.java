package net.airvantage.sched.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.airvantage.sched.app.exceptions.DaoRuntimeException;
import net.airvantage.sched.model.JobConfig;

/**
 * DAO to manage the {@link JobConfig} object model.
 */
public class JobConfigDao {

    private Logger LOG = LoggerFactory.getLogger(JobConfigDao.class);

    private QueryExecutor queryExecutor;

    public JobConfigDao(DataSource dataSource) {
        this.queryExecutor = new QueryExecutor(dataSource);
    }

    /**
     * Persist the given job configuration. If a config with the same id exists it will be updated.
     */
    public void persist(JobConfig config) throws DaoRuntimeException {
        LOG.debug("persist : config={}", config);

        try {
            queryExecutor.update("insert into sched_job_configs(id, url, timeout) values(?, ?, ?) "
                    + "on duplicate key update url=?, timeout=?", config.getId(), config.getUrl(), config.getTimeout(),
                    config.getUrl(), config.getTimeout());

        } catch (SQLException sqlex) {
            throw new DaoRuntimeException(sqlex);
        }
    }

    /**
     * Delete the job configuration identified by the given identifier.
     */
    public void delete(String confId) throws DaoRuntimeException {
        LOG.debug("delete : confId={}", confId);

        try {
            queryExecutor.update("delete from sched_job_configs where id=?", confId);

        } catch (SQLException sqlex) {
            throw new DaoRuntimeException(sqlex);
        }
    }

    /**
     * Return the job configuration identified by the given identifier.
     */
    public JobConfig find(String confId) throws DaoRuntimeException {
        LOG.debug("find : confId={}", confId);

        try {
            ResultSetHandler<JobConfig> rsh = new ResultSetHandler<JobConfig>() {
                @Override
                public JobConfig handle(ResultSet rs) throws SQLException {

                    if (!rs.next()) {
                        return null;
                    }

                    JobConfig config = new JobConfig();
                    config.setId((String) rs.getString(1));
                    config.setUrl((String) rs.getString(2));
                    config.setTimeout((Long) rs.getLong(3));

                    return config;

                }
            };

            return queryExecutor.query("select id, url, timeout from sched_job_configs where id=?", rsh, confId);

        } catch (SQLException sqlex) {
            throw new DaoRuntimeException(sqlex);
        }
    }

    /**
     * Return all the existing job configurations grouped by their identifier.
     */
    public Map<String, JobConfig> findAll() throws DaoRuntimeException {
        LOG.debug("findAll");

        try {
            ResultSetHandler<Map<String, JobConfig>> rsh = new ResultSetHandler<Map<String, JobConfig>>() {
                @Override
                public Map<String, JobConfig> handle(ResultSet rs) throws SQLException {

                    Map<String, JobConfig> map = new HashMap<String, JobConfig>();

                    while (rs.next()) {
                        JobConfig config = new JobConfig();
                        String id = (String) rs.getString(1);
                        config.setId(id);
                        config.setUrl((String) rs.getString(2));
                        config.setTimeout((Long) rs.getLong(3));
                        map.put(id, config);
                    }

                    return map;
                }
            };
            return queryExecutor.query("select id,url,timeout from sched_job_configs", rsh);

        } catch (SQLException sqlex) {
            throw new DaoRuntimeException(sqlex);
        }

    }

    /**
     * Delete all the existing job configurations.
     */
    public void deleteAll() throws DaoRuntimeException {
        LOG.debug("deleteAll");

        try {
            queryExecutor.update("delete from sched_job_configs");

        } catch (SQLException sqlex) {
            throw new DaoRuntimeException(sqlex);
        }
    }

}
