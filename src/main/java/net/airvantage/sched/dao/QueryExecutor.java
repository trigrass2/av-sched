package net.airvantage.sched.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to execute SQL query using a {@link QueryRunner}.<br>
 * SQL queries are retried a maximum of 3 times when a SQLException is raised.
 *
 * @see QueryRunner
 */
public class QueryExecutor {

    private final static Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);

    /** The maximum number of retries that will be done on failure. */
    private static final int DEFAULT_MAX_RETRIES = 3;

    /** Minimum time to wait before starting a retry (in milliseconds). */
    private static final int MIN_DELAY = 50;

    /** Maximum time to wait before starting a retry (in milliseconds). */
    private static final int MAX_DELAY = 100;

    private QueryRunner queryRunner;

    /**
     * Constructor for QueryExecutor that takes a <code>DataSource</code> to use.
     *
     * Methods that do not take a <code>Connection</code> parameter will retrieve connections from this
     * <code>DataSource</code>.
     *
     * @param ds The <code>DataSource</code> to retrieve connections from.
     */
    public QueryExecutor(DataSource ds) {
        this.queryRunner = new QueryRunner(ds);
    }

    /**
     * Constructor for QueryExecutor that takes a <code>QueryRunner</code> to use.
     *
     * @param queryRunner The <code>QueryRunner</code> to use.
     */
    public QueryExecutor(QueryRunner queryRunner) {
        this.queryRunner = queryRunner;
    }

    /**
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
     *
     * @param conn The Connection to use to run the query. The caller is responsible for closing this Connection.
     * @param sql The SQL to execute.
     * @param params An array of query replacement parameters. Each row in this array is one set of batch replacement
     *        values.
     * @return The number of rows updated per statement.
     * @throws SQLException if a database access error occurs
     * @since DbUtils 1.1
     */
    public int[] batch(Connection conn, String sql, Object[][] params) throws SQLException {

        return execute(() -> queryRunner.batch(conn, sql, params));
    }

    /**
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries. The <code>Connection</code> is retrieved from the
     * <code>DataSource</code> set in the constructor. This <code>Connection</code> must be in auto-commit mode or the
     * update will not be saved.
     *
     * @param sql The SQL to execute.
     * @param params An array of query replacement parameters. Each row in this array is one set of batch replacement
     *        values.
     * @return The number of rows updated per statement.
     * @throws SQLException if a database access error occurs
     * @since DbUtils 1.1
     */
    public int[] batch(String sql, Object[][] params) throws SQLException {

        return execute(() -> queryRunner.batch(sql, params));
    }

    /**
     * Execute an SQL SELECT query with replacement parameters. The caller is responsible for closing the connection.
     * 
     * @param <T> The type of object that the handler returns
     * @param conn The connection to execute the query in.
     * @param sql The query to execute.
     * @param rsh The handler that converts the results into an object.
     * @param params The replacement parameters.
     * @return The object returned by the handler.
     * @throws SQLException if a database access error occurs
     */
    public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {

        return execute(() -> queryRunner.query(conn, sql, rsh, params));
    }

    /**
     * Execute an SQL SELECT query without any replacement parameters. The caller is responsible for closing the
     * connection.
     * 
     * @param <T> The type of object that the handler returns
     * @param conn The connection to execute the query in.
     * @param sql The query to execute.
     * @param rsh The handler that converts the results into an object.
     * @return The object returned by the handler.
     * @throws SQLException if a database access error occurs
     */
    public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh) throws SQLException {

        return execute(() -> queryRunner.query(conn, sql, rsh));
    }

    /**
     * Executes the given SELECT SQL query and returns a result object. The <code>Connection</code> is retrieved from
     * the <code>DataSource</code> set in the constructor.
     * 
     * @param <T> The type of object that the handler returns
     * @param sql The SQL statement to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code>.
     * @param params Initialize the PreparedStatement's IN parameters with this array.
     * @return An object generated by the handler.
     * @throws SQLException if a database access error occurs
     */
    public <T> T query(String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {

        return execute(() -> queryRunner.query(sql, rsh, params));
    }

    /**
     * Executes the given SELECT SQL without any replacement parameters. The <code>Connection</code> is retrieved from
     * the <code>DataSource</code> set in the constructor.
     * 
     * @param <T> The type of object that the handler returns
     * @param sql The SQL statement to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code>.
     *
     * @return An object generated by the handler.
     * @throws SQLException if a database access error occurs
     */
    public <T> T query(String sql, ResultSetHandler<T> rsh) throws SQLException {

        return execute(() -> queryRunner.query(sql, rsh));
    }

    /**
     * Execute an SQL INSERT, UPDATE, or DELETE query without replacement parameters.
     *
     * @param conn The connection to use to run the query.
     * @param sql The SQL to execute.
     * @return The number of rows updated.
     * @throws SQLException if a database access error occurs
     */
    public int update(Connection conn, String sql) throws SQLException {

        return execute(() -> queryRunner.update(conn, sql));
    }

    /**
     * Execute an SQL INSERT, UPDATE, or DELETE query with a single replacement parameter.
     *
     * @param conn The connection to use to run the query.
     * @param sql The SQL to execute.
     * @param param The replacement parameter.
     * @return The number of rows updated.
     * @throws SQLException if a database access error occurs
     */
    public int update(Connection conn, String sql, Object param) throws SQLException {

        return execute(() -> queryRunner.update(conn, sql, param));
    }

    /**
     * Execute an SQL INSERT, UPDATE, or DELETE query.
     *
     * @param conn The connection to use to run the query.
     * @param sql The SQL to execute.
     * @param params The query replacement parameters.
     * @return The number of rows updated.
     * @throws SQLException if a database access error occurs
     */
    public int update(Connection conn, String sql, Object... params) throws SQLException {

        return execute(() -> queryRunner.update(conn, sql, params));
    }

    /**
     * Executes the given INSERT, UPDATE, or DELETE SQL statement without any replacement parameters. The
     * <code>Connection</code> is retrieved from the <code>DataSource</code> set in the constructor. This
     * <code>Connection</code> must be in auto-commit mode or the update will not be saved.
     *
     * @param sql The SQL statement to execute.
     * @throws SQLException if a database access error occurs
     * @return The number of rows updated.
     */
    public int update(String sql) throws SQLException {

        return execute(() -> queryRunner.update(sql));
    }

    /**
     * Executes the given INSERT, UPDATE, or DELETE SQL statement with a single replacement parameter. The
     * <code>Connection</code> is retrieved from the <code>DataSource</code> set in the constructor. This
     * <code>Connection</code> must be in auto-commit mode or the update will not be saved.
     *
     * @param sql The SQL statement to execute.
     * @param param The replacement parameter.
     * @throws SQLException if a database access error occurs
     * @return The number of rows updated.
     */
    public int update(String sql, Object param) throws SQLException {

        return execute(() -> queryRunner.update(sql, param));
    }

    /**
     * Executes the given INSERT, UPDATE, or DELETE SQL statement. The <code>Connection</code> is retrieved from the
     * <code>DataSource</code> set in the constructor. This <code>Connection</code> must be in auto-commit mode or the
     * update will not be saved.
     *
     * @param sql The SQL statement to execute.
     * @param params Initializes the PreparedStatement's IN (i.e. '?') parameters.
     * @throws SQLException if a database access error occurs
     * @return The number of rows updated.
     */
    public int update(String sql, Object... params) throws SQLException {

        return execute(() -> queryRunner.update(sql, params));
    }

    /**
     * Executes the given INSERT SQL without any replacement parameters. The <code>Connection</code> is retrieved from
     * the <code>DataSource</code> set in the constructor.
     * 
     * @param <T> The type of object that the handler returns
     * @param sql The SQL statement to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code> of auto-generated keys.
     * @return An object generated by the handler.
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    public <T> T insert(String sql, ResultSetHandler<T> rsh) throws SQLException {

        return execute(() -> queryRunner.insert(sql, rsh));
    }

    /**
     * Executes the given INSERT SQL statement. The <code>Connection</code> is retrieved from the
     * <code>DataSource</code> set in the constructor. This <code>Connection</code> must be in auto-commit mode or the
     * insert will not be saved.
     * 
     * @param <T> The type of object that the handler returns
     * @param sql The SQL statement to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code> of auto-generated keys.
     * @param params Initializes the PreparedStatement's IN (i.e. '?')
     * @return An object generated by the handler.
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    public <T> T insert(String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {

        return execute(() -> queryRunner.insert(sql, rsh, params));
    }

    /**
     * Execute an SQL INSERT query without replacement parameters.
     * 
     * @param <T> The type of object that the handler returns
     * @param conn The connection to use to run the query.
     * @param sql The SQL to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code> of auto-generated keys.
     * @return An object generated by the handler.
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    public <T> T insert(Connection conn, String sql, ResultSetHandler<T> rsh) throws SQLException {

        return execute(() -> queryRunner.insert(conn, sql, rsh));
    }

    /**
     * Execute an SQL INSERT query.
     * 
     * @param <T> The type of object that the handler returns
     * @param conn The connection to use to run the query.
     * @param sql The SQL to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code> of auto-generated keys.
     * @param params The query replacement parameters.
     * @return An object generated by the handler.
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    public <T> T insert(Connection conn, String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {

        return execute(() -> queryRunner.insert(conn, sql, rsh, params));
    }

    /**
     * Executes the given batch of INSERT SQL statements. The <code>Connection</code> is retrieved from the
     * <code>DataSource</code> set in the constructor. This <code>Connection</code> must be in auto-commit mode or the
     * insert will not be saved.
     * 
     * @param <T> The type of object that the handler returns
     * @param sql The SQL statement to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code> of auto-generated keys.
     * @param params Initializes the PreparedStatement's IN (i.e. '?')
     * @return The result generated by the handler.
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    public <T> T insertBatch(String sql, ResultSetHandler<T> rsh, Object[][] params) throws SQLException {

        return execute(() -> queryRunner.insertBatch(sql, rsh, params));
    }

    /**
     * Executes the given batch of INSERT SQL statements.
     * 
     * @param <T> The type of object that the handler returns
     * @param conn The connection to use to run the query.
     * @param sql The SQL to execute.
     * @param rsh The handler used to create the result object from the <code>ResultSet</code> of auto-generated keys.
     * @param params The query replacement parameters.
     * @return The result generated by the handler.
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    public <T> T insertBatch(Connection conn, String sql, ResultSetHandler<T> rsh, Object[][] params)
            throws SQLException {

        return execute(() -> queryRunner.insertBatch(conn, sql, rsh, params));
    }

    /**
     * Execute the given query and return its result.<br>
     * The query is retried a maximum of 3 times when a SQLException is raised.
     */
    private <T> T execute(Query<T> query) throws SQLException {
        SQLException error = null;
        int numAttempts = 0;

        do {
            if (numAttempts > 0) {
                // Pause before a new attempt
                try {
                    int delay = MIN_DELAY + ThreadLocalRandom.current().nextInt(MAX_DELAY - MIN_DELAY);
                    Thread.sleep(delay);
                } catch (Exception ex) {
                    // Ignore
                }
            }

            numAttempts++;

            try {
                return query.execute();

            } catch (SQLException ex) {
                error = ex;

                LOG.warn("Error trying to execute a SQL query (#{} of {})", ex);
            }

        } while (numAttempts < DEFAULT_MAX_RETRIES);

        LOG.warn("Error trying to execute a SQL query (max retries reached)", error);
        throw error;
    }

    /**
     * Interface used to define a SQL query
     *
     * @param <T> the type of the result
     */
    private interface Query<T> {

        T execute() throws SQLException;
    }
}
