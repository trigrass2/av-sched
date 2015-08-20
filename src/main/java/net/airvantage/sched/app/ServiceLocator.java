package net.airvantage.sched.app;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import javax.sql.DataSource;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.app.exceptions.ServiceRuntimeException;
import net.airvantage.sched.app.mapper.JsonMapper;
import net.airvantage.sched.conf.ConfigurationManager;
import net.airvantage.sched.conf.Keys;
import net.airvantage.sched.dao.JobConfigDao;
import net.airvantage.sched.dao.JobLockDao;
import net.airvantage.sched.dao.JobSchedulingDao;
import net.airvantage.sched.dao.JobWakeupDao;
import net.airvantage.sched.db.SchemaMigrator;
import net.airvantage.sched.quartz.DefaultJobListener;
import net.airvantage.sched.quartz.DefaultTriggerListener;
import net.airvantage.sched.quartz.QuartzClusteredSchedulerFactory;
import net.airvantage.sched.services.JobSchedulingService;
import net.airvantage.sched.services.JobStateService;
import net.airvantage.sched.services.impl.JobSchedulingServiceImpl;
import net.airvantage.sched.services.impl.JobStateServiceImpl;
import net.airvantage.sched.services.tech.JobExecutionHelper;
import net.airvantage.sched.services.tech.RetryPolicyHelper;
import net.airvantage.sched.tech.AutoRetryStrategyImpl;

import org.apache.commons.configuration.Configuration;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLocator {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceLocator.class);

    // Do not use everywhere, only in things like quartz jobs & servlets.
    private static ServiceLocator instance;

    private ConfigurationManager configManager;
    private Scheduler scheduler;
    private SchemaMigrator schemaMigrator;
    private DataSource dataSource;
    private JsonMapper jsonMapper;
    private CloseableHttpClient httpClient;

    private JobStateService jobStateService;
    private JobSchedulingService jobService;
    private RetryPolicyHelper retryPolicyHelper;
    private JobExecutionHelper jobExecutionHelper;

    private JobSchedulingDao jobSchedulingDao;
    private JobConfigDao jobConfigDao;
    private JobWakeupDao jobWakeupDao;
    private JobLockDao jobLockDao;

    // ----------------------------------------------- Initialization -------------------------------------------------

    public static ServiceLocator getInstance() {
        if (instance == null) {
            instance = new ServiceLocator();
        }
        return instance;
    }

    public void init() {

        instance = this;
        configManager = new ConfigurationManager();
    }

    public void servicesPreload() throws AppException {

        // Load internal jobs
        ((JobSchedulingServiceImpl) getJobSchedulingService()).loadInternalJobs();
    }

    // -------------------------------------------------- Services ----------------------------------------------------

    public JobSchedulingService getJobSchedulingService() {
        if (jobService == null) {
            jobService = new JobSchedulingServiceImpl(getScheduler(), getJobStateService(), getJobConfigDao(),
                    getJobLockDao(), getJobSchedulingDao(), getJobWakeupDao());
        }
        return jobService;
    }

    public RetryPolicyHelper getRetryPolicyHelper() {
        if (retryPolicyHelper == null) {
            retryPolicyHelper = new RetryPolicyHelper(getJobStateService(), getJobSchedulingService(),
                    getJobWakeupDao());
        }
        return retryPolicyHelper;
    }

    public JobStateService getJobStateService() {
        if (jobStateService == null) {
            jobStateService = new JobStateServiceImpl(getJobConfigDao(), getJobLockDao(), getJobSchedulingDao());

        }
        return jobStateService;
    }

    public JobSchedulingDao getJobSchedulingDao() {
        if (jobSchedulingDao == null) {
            jobSchedulingDao = new JobSchedulingDao(getScheduler());

        }
        return jobSchedulingDao;
    }

    public JobLockDao getJobLockDao() {
        if (jobLockDao == null) {
            jobLockDao = new JobLockDao(getDataSource());

        }
        return jobLockDao;
    }

    public JobConfigDao getJobConfigDao() {
        if (jobConfigDao == null) {
            jobConfigDao = new JobConfigDao(getDataSource());

        }
        return jobConfigDao;
    }

    public JobWakeupDao getJobWakeupDao() {
        if (jobWakeupDao == null) {
            jobWakeupDao = new JobWakeupDao(getDataSource());

        }
        return jobWakeupDao;
    }

    public JobExecutionHelper geJobExecutionHelper() {
        if (jobExecutionHelper == null) {
            jobExecutionHelper = new JobExecutionHelper(getJobStateService(), getHttpClient(), getSchedSecret(),
                    getJsonMapper(), getJobConfigDao(), getRetryPolicyHelper());
        }
        return jobExecutionHelper;
    }

    public CloseableHttpClient getHttpClient() {
        if (httpClient == null) {

            AutoRetryStrategyImpl retryStartegy = new AutoRetryStrategyImpl(5, 1000, new HashSet<Integer>(
                    Arrays.asList(503, 504)));

            httpClient = HttpClientBuilder.create().disableContentCompression().setMaxConnPerRoute(10)
                    .setMaxConnTotal(25).evictExpiredConnections().setServiceUnavailableRetryStrategy(retryStartegy)
                    .build();
        }
        return httpClient;
    }

    public ConfigurationManager getConfigManager() {
        return configManager;
    }

    public JsonMapper getJsonMapper() {
        if (jsonMapper == null) {
            jsonMapper = new JsonMapper();
        }
        return jsonMapper;
    }

    public SchemaMigrator getSchemaMigrator() {
        if (schemaMigrator == null) {
            schemaMigrator = new SchemaMigrator(getDataSource());
        }
        return schemaMigrator;
    }

    public Scheduler getScheduler() {
        if (scheduler == null) {
            try {
                scheduler = QuartzClusteredSchedulerFactory.buildScheduler(getConfigManager().get());

                scheduler.start();
                scheduler.getListenerManager().addTriggerListener(getLockTriggerListener());
                scheduler.getListenerManager().addJobListener(getRetryJobListener());

            } catch (SchedulerException ex) {
                LOG.error("Unable to load scheduler", ex);
                throw new ServiceRuntimeException("Unable to load scheduler", ex);
            }
        }
        return scheduler;
    }

    public String getSchedSecret() {
        return getConfigManager().get().getString("av-sched.secret");
    }

    // ---------------------------------------------------- Private Methods -------------------------------------------

    private DataSource getDataSource() {
        if (dataSource == null) {
            Configuration config = getConfigManager().get();
            com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource ds = new com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource();
            ds.setServerName(config.getString(Keys.Db.SERVER));
            ds.setPortNumber(config.getInt(Keys.Db.PORT));
            ds.setDatabaseName(config.getString(Keys.Db.DB_NAME));
            ds.setUser(config.getString(Keys.Db.USER));
            ds.setPassword(config.getString(Keys.Db.PASSWORD));
            // Attempt to fix https://github.com/AirVantage/av-sched/issues/6
            ds.setAutoReconnect(true);

            dataSource = ds;
        }
        return dataSource;
    }

    private TriggerListener getLockTriggerListener() {
        return new DefaultTriggerListener(getJobStateService());
    }

    private JobListener getRetryJobListener() {
        return new DefaultJobListener(getRetryPolicyHelper());
    }

    public int getPort() {
        return getConfigManager().get().getInt("av-sched.port");
    }
}
