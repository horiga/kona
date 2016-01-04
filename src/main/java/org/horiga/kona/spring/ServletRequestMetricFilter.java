package org.horiga.kona.spring;


import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.annotation.PostConstruct;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ServletRequestMetricFilter extends OncePerRequestFilter {

	private static Map<String, Counter> monitors;

	private static Map<HttpStatus, Counter> errorMonitors;

	@PostConstruct
	public void init() {
		monitors = new HashMap<>();
		monitors.put("unknown", Monitors.newCounter("servlet-http-status-unknown"));
		Arrays.asList(HttpStatus.Series.values())
				.stream()
				.forEach(s -> monitors.put(s.name().toLowerCase(), Monitors.newCounter("servlet-http-status-" + s.name().toLowerCase())));
		errorMonitors = new HashMap<>();
		Arrays.asList(HttpStatus.values())
				.stream()
				.filter(s -> containsErrorStatusMonitor(s))
				.forEach(s -> errorMonitors.put(s, Monitors.newCounter("servlet-errors-" + s.value())));
	}

	@Override
	public boolean shouldNotFilterAsyncDispatch() {
		return false;
	}

	@Override
	protected void doFilterInternal(final HttpServletRequest servletRequest,
									final HttpServletResponse servletResponse,
									final FilterChain filterChain) throws ServletException, IOException {
		try {
			filterChain.doFilter(servletRequest, servletResponse);
		} finally {
			if (!isAsyncStarted(servletRequest)) {
				markMetrics(servletRequest, servletResponse);
			}
		}
	}

	protected void markMetrics(final HttpServletRequest servletRequest,
                               final HttpServletResponse servletResponse) {
		String seriesName = "unknown";
		Optional<HttpStatus> status = status(servletResponse);
        if (status.isPresent()) {
            if ( containsErrorStatusMonitor(status.get())) {
                errorMonitors.get(status.get()).increment();
            }
            HttpStatus.Series series = status.get().series();
            if (!Objects.isNull(series)) {
                seriesName = series.name();
            }
        }
		monitors.get(seriesName.toLowerCase()).increment();
	}

	private Optional<HttpStatus> status(HttpServletResponse response) {
		try {
			return Optional.of(HttpStatus.valueOf(response.getStatus()));
		}catch (Exception e) {
			// IllegalArgumentException
			return Optional.empty();
		}
	}

    private boolean containsErrorStatusMonitor(HttpStatus s) {
        return s.is4xxClientError() || s.is5xxServerError();
    }

}
