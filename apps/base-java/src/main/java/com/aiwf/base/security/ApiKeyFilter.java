package com.aiwf.base.security;

import com.aiwf.base.config.AppProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
public class ApiKeyFilter extends OncePerRequestFilter {
    private static final String ACTUATOR_HEALTH_PATH = "/actuator/health";

    private final AppProperties props;

    public ApiKeyFilter(AppProperties props) {
        this.props = props;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getServletPath();
        if (path == null || path.isBlank()) {
            path = request.getRequestURI();
        }
        return path != null
                && (ACTUATOR_HEALTH_PATH.equals(path) || path.startsWith(ACTUATOR_HEALTH_PATH + "/"));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {

        String required = props.getApiKey();
        if (required != null && !required.isBlank()) {
            String got = req.getHeader("X-API-Key");
            if (!required.equals(got)) {
                res.setStatus(401);
                res.setContentType("application/json; charset=utf-8");
                res.getWriter().write("{\"ok\":false,\"error\":\"unauthorized\"}");
                return;
            }
        }
        chain.doFilter(req, res);
    }
}
