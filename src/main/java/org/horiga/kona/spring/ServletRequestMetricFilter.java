package org.horiga.kona.spring;


import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;
import lombok.extern.slf4j.Slf4j;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.annotation.PostConstruct;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class ServletRequestMetricFilter extends OncePerRequestFilter {

	private static Map<String, Counter> monitors;

	@PostConstruct
	public void init() {
		monitors = new HashMap<>();
		monitors.put("2xx", Monitors.newCounter("servlet-http-status-2xx"));
		monitors.put("3xx", Monitors.newCounter("servlet-http-status-3xx"));
		monitors.put("4xx", Monitors.newCounter("servlet-http-status-4xx"));
		monitors.put("5xx", Monitors.newCounter("servlet-http-status-5xx"));
		monitors.put("xxx", Monitors.newCounter("servlet-http-status-unknown"));
	}

	@Override
	protected void doFilterInternal(final HttpServletRequest servletRequest,
									final HttpServletResponse servletResponse,
									final FilterChain filterChain) throws ServletException, IOException {
		try {
			if (Objects.isNull(servletRequest.getAttribute("servlet-metric-session-id")))
				servletRequest.setAttribute("servlet-metric-session-id", UUID.randomUUID().toString());
			filterChain.doFilter(servletRequest, servletResponse);
		} finally {
			if( null == monitors) {
				// Do not anything.
				return;
			}
			if (isAsyncStarted(servletRequest)) {
				AsyncContext ctx = servletRequest.getAsyncContext();
				if(!Objects.isNull(ctx)) {
					ctx.addListener(new AsyncListener() {
						@Override
						public void onComplete(AsyncEvent event) throws IOException {
							log.debug("async-servlet: onComplete");
							serve(servletRequest, servletResponse);
						}

						@Override
						public void onTimeout(AsyncEvent event) throws IOException {
							log.debug("async-servlet: onTimeout");
							serve(servletRequest, servletResponse);
						}

						@Override
						public void onError(AsyncEvent event) throws IOException {
							log.debug("async-servlet: onError");
							serve(servletRequest, servletResponse);
						}

						@Override
						public void onStartAsync(AsyncEvent event) throws IOException {
							log.debug("async-servlet: onStartAsync");
						}
					});
				} else {
					//
				}
			} else {
				serve(servletRequest, servletResponse);
			}
		}
	}

	protected void serve(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
		int sc = servletResponse.getStatus();
		if ( sc >= 200) {
			int n = sc / 100;
			String key = (n <= 5) ? n + "xx" : "xxx";
			monitors.get(key).increment();
		} else {
			// monitors.get("xxx").increment();
		}
	}

}
