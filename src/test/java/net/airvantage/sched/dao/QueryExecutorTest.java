package net.airvantage.sched.dao;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@SuppressWarnings("unchecked")
public class QueryExecutorTest {

    private QueryExecutor queryExecutor;

    @Mock
    private QueryRunner queryRunner;

    @Before
    public void setUp() {

        MockitoAnnotations.initMocks(this);
        queryExecutor = new QueryExecutor(queryRunner);
    }

    @Test
    public void test_no_retry() throws Exception {

        ResultSetHandler<String> rsh = mock(ResultSetHandler.class);
        String sql = "sql";

        when(queryRunner.query(sql, rsh)).thenReturn("ok");

        String res = queryExecutor.query(sql, rsh);
        assertEquals("ok", res);

        verify(queryRunner, times(1)).query(sql, rsh);
    }

    @Test
    public void test_success_after_one_retry() throws Exception {

        ResultSetHandler<String> rsh = mock(ResultSetHandler.class);
        String sql = "sql";

        SQLException ex1 = new SQLException("error 1");
        when(queryRunner.query(sql, rsh)).thenThrow(ex1).thenReturn("ok");

        String res = queryExecutor.query(sql, rsh);
        assertEquals("ok", res);

        verify(queryRunner, times(2)).query(sql, rsh);
    }

    @Test
    public void test_success_after_two_retry() throws Exception {

        ResultSetHandler<String> rsh = mock(ResultSetHandler.class);
        String sql = "sql";

        SQLException ex1 = new SQLException("error 1");
        SQLException ex2 = new SQLException("error 2");
        when(queryRunner.query(sql, rsh)).thenThrow(ex1).thenThrow(ex2).thenReturn("ok");

        String res = queryExecutor.query(sql, rsh);
        assertEquals("ok", res);

        verify(queryRunner, times(3)).query(sql, rsh);
    }

    @Test
    public void test_max_retry() throws Exception {

        ResultSetHandler<String> rsh = mock(ResultSetHandler.class);
        String sql = "sql";

        SQLException ex1 = new SQLException("error 1");
        SQLException ex2 = new SQLException("error 2");
        SQLException ex3 = new SQLException("error 3");
        when(queryRunner.query(sql, rsh)).thenThrow(ex1, ex2, ex3);

        try {
            queryExecutor.query(sql, rsh);
            fail("SQLException should be raised");
        } catch (SQLException e) {
            assertEquals(ex3, e);
        }

        verify(queryRunner, times(3)).query(sql, rsh);
    }
}
