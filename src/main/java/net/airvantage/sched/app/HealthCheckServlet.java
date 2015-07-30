package net.airvantage.sched.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import net.airvantage.sched.app.mapper.JsonMapper;

public class HealthCheckServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private JsonMapper jsonMapper;
    private Scheduler scheduler;

    @Override
    public void init() throws ServletException {

        jsonMapper = ServiceLocator.getInstance().getJsonMapper();
        scheduler = ServiceLocator.getInstance().getScheduler();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Map<String, Object> map = new HashMap<String, Object>();
        try {
            map.put("quartz.started", scheduler.getMetaData().isStarted());
            map.put("quartz.started.from", scheduler.getMetaData().getRunningSince());
            map.put("quartz.pool.size", scheduler.getMetaData().getThreadPoolSize());
            map.put("quartz.nb.job.executed", scheduler.getMetaData().getNumberOfJobsExecuted());

            map.put("app.status", "OK");

        } catch (SchedulerException e) {
            map.put("app.status", "KO - " + e.getMessage());
        }

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json");
        resp.getWriter().println(jsonMapper.writeValueAsString(map));
    }

}
