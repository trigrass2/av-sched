package net.airvantage.sched.app;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.app.mapper.JsonMapper;
import net.airvantage.sched.model.JobState;
import net.airvantage.sched.services.JobStateService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet to access current scheduled jobs state.
 * 
 * <ul>
 * <li>GET - / : the state of the job.</li>
 * <li>GET - / : the state of all scheduled jobs.</li>
 * </ul>
 */
public class JobStateServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    public static final Logger LOG = LoggerFactory.getLogger(JobStateServlet.class);

    private JobStateService jobStateService;
    private JsonMapper jsonMapper;

    @Override
    public void init() throws ServletException {

        jobStateService = ServiceLocator.getInstance().getJobStateService();
        jsonMapper = ServiceLocator.getInstance().getJsonMapper();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String content = null;

        try {
            List<JobState> jobStates = null;
            String jobId = req.getParameter("jobId");

            if (jobId != null) {
                jobStates = Arrays.asList(jobStateService.find(jobId));

            } else {
                jobStates = jobStateService.findAll();
            }

            content = jsonMapper.writeValueAsString(jobStates);

        } catch (AppException e) {
            LOG.debug("Exception while getting jobs state", e);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        resp.setContentType("application/json");
        resp.getWriter().write(content);
    }

}
