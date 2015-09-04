package net.airvantage.sched.app;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                    getJobLockDao(), getJobSchedulingDao(), getJobWakeupDao(), getWakeupJobCron());
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

            int poolSize = this.getOutputCnxPoolSize();

            httpClient = HttpClientBuilder.create().disableContentCompression().setMaxConnPerRoute(poolSize)
                    .setMaxConnTotal(poolSize * 2).evictExpiredConnections()
                    .setServiceUnavailableRetryStrategy(retryStartegy).build();
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

    // ------------------------------------------------- Deploy Configuration -----------------------------------------

    public String getSchedSecret() {
        return getConfigManager().get().getString("av-sched.secret");
    }

    public int getPort() {
        return getConfigManager().get().getInt("av-sched.port");
    }

    public int getOutputCnxPoolSize() {
        return getConfigManager().get().getInt(Keys.Io.OUT_CNX_POOL_SIZE, 100);
    }

    public int getWakeupJobThreadPoolSize() {

        return getConfigManager().get().getInt(Keys.Io.OUT_THREAD_POOL_SIZE, 100);
    }

    public int getServletCnxPoolSize() {
        return getConfigManager().get().getInt(Keys.Io.IN_CNX_POOL_SIZE, 60);
    }

    public int getDbCnxPoolMin() {
        return getConfigManager().get().getInt(Keys.Db.POOL_MIN, 50);
    }

    public int getDbCnxPoolMax() {
        return getConfigManager().get().getInt(Keys.Db.POOL_MAX, 200);
    }

    public String getWakeupJobCron() {
        return getConfigManager().get().getString(Keys.Cron.WAKEUP_JOB, "0/10 * * * * ?");
    }

    // ---------------------------------------------------- Private Methods -------------------------------------------

    private DataSource getDataSource() {
        if (dataSource == null) {

            String host = getConfigManager().get().getString(Keys.Db.SERVER);
            int port = getConfigManager().get().getInt(Keys.Db.PORT);
            String dbname = getConfigManager().get().getString(Keys.Db.DB_NAME);
            String user = getConfigManager().get().getString(Keys.Db.USER);
            String password = getConfigManager().get().getString(Keys.Db.PASSWORD);

            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("defaultTransactionIsolation", "NONE");

            String url = "jdbc:mysql://" + host + ":" + port + "/" + dbname + "?tcpKeepAlive=true";

            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            poolConfig.setMinIdle(getDbCnxPoolMin());
            poolConfig.setMaxTotal(getDbCnxPoolMax());

            ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, props);
            PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
            GenericObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory, poolConfig);
            poolableConnectionFactory.setPool(connectionPool);

            dataSource = new PoolingDataSource<>(connectionPool);

            LOG.info("Starting wakeup datasource with maxTotal={}", connectionPool.getMaxTotal());
        }

        return dataSource;
    }

    private TriggerListener getLockTriggerListener() {
        return new DefaultTriggerListener(getJobStateService());
    }

    private JobListener getRetryJobListener() {
        return new DefaultJobListener(getRetryPolicyHelper());
    }
}
