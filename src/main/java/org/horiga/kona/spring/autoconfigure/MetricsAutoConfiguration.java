package org.horiga.kona.spring.autoconfigure;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.horiga.kona.reporter.ElasticsearchReporter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.MetricRepositoryAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Configuration
@AutoConfigureAfter(MetricRepositoryAutoConfiguration.class)
public class MetricsAutoConfiguration {

	private final long THREAD_STATES_INTERVAL_MILLIS = 5000L;

	public static final String JVM_MEMORY_METRIC_GAUGES = "jvm.memory";
	public static final String JVM_THREADS_METRIC_GAUGES = "jvm.threads";
	public static final String JVM_GARBAGE_COLLECTOR_METRIC_GAUGES = "jvm.gc";
	public static final String JVM_BUFFERPOOLS_METRIC_GAUGES = "jvm.buffers";

	@Value("${kona.metric.project:default")
	private String project;

	/**
	 * 'memory', 'threads', 'gc', 'buffers'
	 */
	@Value("${kona.metric.jvm:memory,threads")
	private String jvmMetrics;

	@Value("${kona.metric.reporter.intervalMillis:10000}")
	private long intervalMillis;

	@Value("${kona.metric.reporter.elasticsearch.indexes.prefix:metrics-}")
	private String elasticsearchIndexesPrefix;

	@Value("${kona.metric.reporter.elasticsearch.hosts:NONE}")
	private String elasticsearch;

	@Bean
	@ConditionalOnMissingBean
	MetricFilter metricFilter() {
		return MetricFilter.ALL;
	}

	@Bean
	@ConditionalOnMissingBean
	public ElasticsearchReporter elasticsearchReporter(MetricRegistry registry, MetricFilter filter) {

		registerJvmMetrics(registry);

		final Map<String, String> additionalFields = Maps.newHashMap();
		additionalFields.put("project", project);
		ElasticsearchReporter reporter = ElasticsearchReporter.forRegistry(registry, elasticsearchIndexesPrefix)
			.filter(filter)
			.enabled(!"NONE".equals(elasticsearch))
			.additionalFields(additionalFields)
			.withResettable()
			.build(elasticsearch);
		reporter.start(intervalMillis, TimeUnit.MILLISECONDS);
		return reporter;
	}

	private void registerJvmMetrics(MetricRegistry registry) {
		Splitter.on(",").omitEmptyStrings().trimResults().splitToList(jvmMetrics)
			.stream().forEach(s -> {
				if ("memory".equals(s)) {
					registerMetricSet(registry, JVM_MEMORY_METRIC_GAUGES, new MemoryUsageGaugeSet());
				} else if ("threads".equals(s)) {
					registerMetricSet(registry, JVM_THREADS_METRIC_GAUGES, new CachedThreadStatesGaugeSet(
						THREAD_STATES_INTERVAL_MILLIS, TimeUnit.MILLISECONDS));
				} else if ("gc".equals(s)) {
					registerMetricSet(registry, JVM_GARBAGE_COLLECTOR_METRIC_GAUGES,
						new GarbageCollectorMetricSet());

				} else if ("buffers".equals(s)) {
					registerMetricSet(registry, JVM_BUFFERPOOLS_METRIC_GAUGES,
						new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
				}
			});
	}

	private void registerMetricSet(MetricRegistry registry, String name, MetricSet metric) {
		if (registry.getNames().stream().filter(
			s -> s.startsWith(name))
			.findFirst()
			.isPresent())
			registry.register(name, metric);
	}

}
