package au.org.ala.biocache.web;

import org.ala.client.util.Constants;
import org.ala.client.util.LoggingContext;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * HTTP Request Filter to pass the user-agent request header to the ALA Logger Client so it can be included in posts
 * to the logger service
 */
public class LoggerConfigFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        try {
            LoggingContext.setProperty(Constants.USER_AGENT_PARAM, ((HttpServletRequest)request).getHeader(Constants
                    .USER_AGENT_PARAM));

            chain.doFilter(request, response);
        } finally {
           LoggingContext.clearContext();
        }
    }

    @Override
    public void destroy() {
        LoggingContext.clearContext();
    }
}
