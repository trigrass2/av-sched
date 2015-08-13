package net.airvantage.sched.app;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A filter to manage internal communication security.
 * 
 * <p>
 * A secret (see configuration properties) is shared with clients and should be set into the requests headers.
 * </P>
 */
public class SchedSecretFilter implements Filter {

    public static final String SCHED_SECRET_HEADER_NAME = "X-Sched-secret";

    public SchedSecretFilter() {}

    @Override
    public void init(FilterConfig arg0) throws ServletException {}

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException,
            ServletException {

        HttpServletRequest httpReq = (HttpServletRequest) req;
        HttpServletResponse httpResp = (HttpServletResponse) resp;

        String schedSecretHeader = httpReq.getHeader(SCHED_SECRET_HEADER_NAME);

        // Singleton pattern FTW - this make reloading works fine.
        String schedSecret = ServiceLocator.getInstance().getSchedSecret();
        if (schedSecretHeader != null && schedSecret.equalsIgnoreCase(schedSecretHeader)) {
            chain.doFilter(req, resp);

        } else {
            httpResp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @Override
    public void destroy() {}

}
