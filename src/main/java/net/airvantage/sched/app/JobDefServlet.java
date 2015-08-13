package net.airvantage.sched.app;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.app.mapper.JsonMapper;
import net.airvantage.sched.model.JobDef;
import net.airvantage.sched.model.JobId;
import net.airvantage.sched.services.JobSchedulingService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet to create or delete a new scheduling job.
 * 
 * <ul>
 * <li>POST - / : create and schedule a new job.</li>
 * <li>DELETE - / : unschedule a job and delete its configuration.</li> </li>
 * </ul>
 */
public class JobDefServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JobDefServlet.class);

    public static long startupTime;
    public static long startupDuration;

    private JobSchedulingService jobService;
    private JsonMapper jsonMapper;

    @Override
    public void init() throws ServletException {
        super.init();

        jobService = ServiceLocator.getInstance().getJobSchedulingService();
        jsonMapper = ServiceLocator.getInstance().getJsonMapper();
    }

    /**
     * Schedule a new job.
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Map<String, Object> res = new HashMap<String, Object>();
        try {

            JobDef jobDef = jsonMapper.jobDef(req.getInputStream());

            jobService.scheduleJob(jobDef);

            // Return the job id
            res.put("id", jobDef.getConfig().getId());

        } catch (AppException e) {
            LOG.debug("Exception while scheduling job", e);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            res = e.asMap();
        }

        resp.setContentType("application/json");
        resp.getWriter().println(jsonMapper.writeValueAsString(res));
    }

    /**
     * Unschedule a job.
     */
    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Map<String, Object> res = new HashMap<String, Object>();
        try {

            JobId jobId = jsonMapper.jobId(req.getInputStream());
            boolean deleted = jobService.unscheduleJob(jobId.getId());

            res.put("id", jobId.getId());
            res.put("deleted", deleted);

        } catch (AppException e) {
            LOG.debug("Exception while scheduling job", e);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            res = e.asMap();
        }

        resp.setContentType("application/json");
        resp.getWriter().println(jsonMapper.writeValueAsString(res));
    }

}
