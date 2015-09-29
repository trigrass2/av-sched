package net.airvantage.sched.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Persist the given job configuration. If a config with the same id exists no update will be done (it's assumed
     * equal).
     */
    public void persist(JobConfig config) throws SQLException {

        try {
            if (this.find(config.getId()) == null) {
                queryExecutor.update("insert into sched_job_configs(id,url,timeout) values(?,?,?)", config.getId(),
                        config.getUrl(), config.getTimeout());
            }

        } catch (SQLException sqlex) {
            // Hack to manage concurrent calls !
            // If this configuration already exists just ignore it
            if (!sqlex.getMessage().contains("Duplicate entry")) {
                throw sqlex;

            } else {
                LOG.warn("Try to create an existing configuration : {}", sqlex.getMessage());
            }
        }
    }

    /**
     * Delete the job configuration identified by the given identifier.
     */
    public void delete(String jobId) throws SQLException {

        queryExecutor.update("delete from sched_job_configs where id=?", jobId);
    }

    /**
     * Return the job configuration identified by the given identifier.
     */
    public JobConfig find(String id) throws SQLException {

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

        return queryExecutor.query("select id, url, timeout from sched_job_configs where id=?", rsh, id);
    }

    /**
     * Return all the existing job configurations grouped by their identifier.
     */
    public Map<String, JobConfig> findAll() throws SQLException {

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

    }

    /**
     * Delete all the existing job configurations.
     */
    public void deleteAll() throws SQLException {
        queryExecutor.update("delete from sched_job_configs");
    }
}
