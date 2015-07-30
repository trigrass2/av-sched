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
import net.airvantage.sched.model.JobId;
import net.airvantage.sched.services.JobSchedulingService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <ul>
 * <li>POST - /ack : acknowledge a job execution.</li>
 * <li>POST - /trigger : trigger an existing job.</li>
 * </ul>
 */
public class JobActionServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    public static final Logger LOG = LoggerFactory.getLogger(JobActionServlet.class);

    private JobSchedulingService jobService;
    private JsonMapper jsonMapper;

    @Override
    public void init() throws ServletException {
        super.init();

        jobService = ServiceLocator.getInstance().getJobSchedulingService();
        jsonMapper = ServiceLocator.getInstance().getJsonMapper();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        if (req.getPathInfo().endsWith("ack")) {
            ackJob(req, resp);

        } else if (req.getPathInfo().endsWith("trigger")) {
            triggerJob(req, resp);
        }
    }

    private void ackJob(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Map<String, Object> res = new HashMap<String, Object>();
        try {

            JobId jobId = jsonMapper.jobId(req.getInputStream());
            jobService.ackJob(jobId.getId());

        } catch (AppException e) {
            LOG.debug("Exception while acknowledging job", e);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            res = e.asMap();
        }

        this.response(resp, res);
    }

    private void triggerJob(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        Map<String, Object> res = new HashMap<String, Object>();
        try {

            JobId jobId = jsonMapper.jobId(req.getInputStream());
            boolean triggered = jobService.triggerJob(jobId.getId());
            res.put("triggered", triggered);

        } catch (AppException e) {
            LOG.debug("Exception while triggering job", e);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            res = e.asMap();
        }

        this.response(resp, res);
    }

    private void response(HttpServletResponse resp, Map<String, Object> res) throws IOException {

        resp.setContentType("application/json");
        resp.getWriter().println(jsonMapper.writeValueAsString(res));
    }

}
