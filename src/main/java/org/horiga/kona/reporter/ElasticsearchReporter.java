package org.horiga.kona.reporter;


import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ElasticsearchReporter extends ScheduledReporter {
	public static final String DEFAULT_ELASTICSEARCH_INDEX_PREFIX = "logstash-";
	public static final String DEFAULT_TIMESTAMP_FIELD_NAME = "@timestamp";
	public static final int DEFAULT_BULK_LIMIT = 2500;

	private static final String ES_BULK_INDEX_RAW_FORMAT = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\"}}";
	@Deprecated
	private static final String ES_BULK_INDEX_WITH_TTL_RAW_FORMAT = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_ttl\":\"%s\"}}";
	private static final DecimalFormat MONTH_AND_DAY_FORMAT = new DecimalFormat("00");

	private final String indexPrefix;
	private final String timestampFieldName;
	private final String metricPrefixName;
	private final ZoneId timezoneId;
	private final String hostname;

	@Deprecated
	private final Long ttl;
	private final Integer bulkLimit;
	private final JsonFactory jsonFactory;
	private final List<String> endpoint;
	private final Map<String, String> additionalFields;

	private final boolean enabled;
	private final boolean resettable;

	private StringWriter buffer;
	private AtomicInteger nextHost, bulkCounter;

	public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry, DEFAULT_ELASTICSEARCH_INDEX_PREFIX);
	}

	public static Builder forRegistry(MetricRegistry registry, String indexPrefix) {
		return new Builder(registry, indexPrefix);
	}

	public static class Builder {

		private final MetricRegistry registry;
		private final String indexPrefix;

		private MetricFilter filter;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;

		private String hostname;
		private String metricPrefix;
		// @see https://www.elastic.co/guide/en/elasticsearch/reference/2.1/docs-bulk.html#bulk-ttl
		@Deprecated
		private Long ttl;
		private String timezoneId;
		private String timestampFieldName;
		private Integer bulkLimit;
		private Map<String, String> additionalFields;
		private boolean enabled;
		private boolean resettable;

		private Builder(MetricRegistry registry, String indexPrefix) {
			this.registry = registry;
			this.indexPrefix = indexPrefix;
			this.hostname = "";
			this.metricPrefix = "";
			this.filter = MetricFilter.ALL;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.timestampFieldName = DEFAULT_TIMESTAMP_FIELD_NAME;
			this.ttl = 0L;
			this.timezoneId = ZoneId.systemDefault().getId();
			this.bulkLimit = DEFAULT_BULK_LIMIT;
			this.enabled = true;
			this.resettable = false;
		}

		public Builder filter(MetricFilter filter) {
			this.filter = filter;
			return this;
		}

		public Builder rateUnit(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		public Builder durationUnit(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		public Builder hostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		public Builder metricPrefix(String metricPrefix) {
			this.metricPrefix = metricPrefix;
			return this;
		}

		@Deprecated
		public Builder ttl(Long ttl) {
			this.ttl = ttl;
			return this;
		}

		public Builder timezone(String timezoneId) {
			this.timezoneId = timezoneId;
			return this;
		}

		public Builder bulkLimit(Integer bulkLimit) {
			this.bulkLimit = bulkLimit;
			return this;
		}

		public Builder additionalFields(Map<String, String> additionalFields) {
			this.additionalFields = additionalFields;
			return this;
		}

		public Builder enabled(boolean enabled) {
			this.enabled = enabled;
			return this;
		}

		public Builder withResettable() {
			this.resettable = true;
			return this;
		}

		/**
		 * @param elasticsearchEndpoint Elasticsearch nodes with ',' separated value.
		 *                              example: "10.10.10.1:9200,10.10.10.2:9200,10.10.10.3:9200"
		 * @return
		 */
		public ElasticsearchReporter build(String elasticsearchEndpoint) {
			return new ElasticsearchReporter(registry, indexPrefix,
					timestampFieldName, metricPrefix, hostname, ttl, filter, rateUnit,
					durationUnit, ZoneId.of(timezoneId), bulkLimit, additionalFields, enabled, resettable,
					elasticsearchEndpoint);
		}
	}

	protected ElasticsearchReporter(
			MetricRegistry registry,
			String indexPrefix,
			String timestampFieldName,
			String metricPrefixName,
			String hostname,
			Long ttl,
			MetricFilter filter,
			TimeUnit rateUnit,
			TimeUnit durationUnit,
			ZoneId timezoneId,
			Integer bulkLimit,
			Map<String, String> additionalFields,
			boolean enabled,
			boolean resettable,
			String endpoint
	) {

		super(registry, "elasticsearch-reporter", filter, rateUnit, durationUnit);

		String _indexPrefix = Strings.isNullOrEmpty(indexPrefix) ? "logstash-" : indexPrefix;
		if (!_indexPrefix.endsWith("-")) {
			_indexPrefix += "-";
		}
		this.indexPrefix = _indexPrefix;
		this.timestampFieldName = Strings.isNullOrEmpty(timestampFieldName) ? "@timestamp" : timestampFieldName;
		this.metricPrefixName = Strings.isNullOrEmpty(metricPrefixName) ? "" : metricPrefixName;
		if (Strings.isNullOrEmpty(hostname)) {
			try {
				InetAddress inetAddress = InetAddress.getLocalHost();
				hostname = inetAddress.getCanonicalHostName();
				if (Strings.isNullOrEmpty(hostname)) {
					hostname = inetAddress.getHostAddress();
				}
			} catch (UnknownHostException e) {
				hostname = "unknown-host";
			}
		}
		this.ttl = ttl;
		this.hostname = hostname;
		this.timezoneId = null != timezoneId ? timezoneId : ZoneId.systemDefault();
		this.jsonFactory = new JsonFactory();
		this.endpoint = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(endpoint);
		// Unsupported
		this.bulkLimit = bulkLimit > 0 ? bulkLimit : DEFAULT_BULK_LIMIT;
		this.nextHost = new AtomicInteger();
		this.bulkCounter = new AtomicInteger();
		this.buffer = new StringWriter();
		this.additionalFields = null != additionalFields ? additionalFields : new HashMap<>();
		this.enabled = enabled;
		this.resettable = resettable;
		log.info("The elasticsearch-reporter was initialized. 'indexPrefix':{}, " +
						"'timestampFieldName':{}, 'metricPrefixName':{}, 'hostname':{}, " +
						"'ttl':{}, 'rateUnit':{}, 'durationUnit':{}, 'timezoneId':{}, " +
						"'bulkLimit':{}, 'endpoint':{}",
				this.indexPrefix, this.timestampFieldName, this.indexPrefix,
				this.timestampFieldName, this.metricPrefixName, this.hostname,
				this.ttl, rateUnit.name(), durationUnit.name(), this.timezoneId,
				this.bulkLimit, endpoint);

	}

	@Override
	public void report(SortedMap<String, Gauge> gauges,
					   SortedMap<String, Counter> counters,
					   SortedMap<String, Histogram> histograms,
					   SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

		ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.now(), timezoneId);
		String timestamp = zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

		final String index = indexPrefix + zdt.getYear() + "."
				+ MONTH_AND_DAY_FORMAT.format(zdt.getMonthValue()) + "."
				+ MONTH_AND_DAY_FORMAT.format(zdt.getDayOfMonth());

		gauges.forEach((k, v) -> reportGauge(index, timestamp, k, v));
		counters.forEach((k, v) -> reportCounter(index, timestamp, k, v));
		meters.forEach((k, v) -> reportMeter(index, timestamp, k, v));
		timers.forEach((k, v) -> reportTimer(index, timestamp, k, v));

		try {
			if (enabled) {
				sendBulkRequest();
			} else {
				sendBulkRequestDummy();
			}
		} finally {
			this.buffer = new StringWriter();
		}
	}

	private void reportTimer(String index, String timestamp, String name,
							 Timer timer) {
		try (StringWriter writer = new StringWriter()) {
			JsonGenerator json = createAndInitJsonGenerator(writer, timestamp, name);

			final Snapshot snapshot = timer.getSnapshot();

			json.writeNumberField("max", convertDuration(snapshot.getMax()));
			json.writeNumberField("mean", convertDuration(snapshot.getMean()));
			json.writeNumberField("min", convertDuration(snapshot.getMin()));
			json.writeNumberField("stddev", convertDuration(snapshot.getStdDev()));
			json.writeNumberField("p50", convertDuration(snapshot.getMedian()));
			json.writeNumberField("p75", convertDuration(snapshot.get75thPercentile()));
			json.writeNumberField("p95", convertDuration(snapshot.get95thPercentile()));
			json.writeNumberField("p98", convertDuration(snapshot.get98thPercentile()));
			json.writeNumberField("p99", convertDuration(snapshot.get99thPercentile()));
			json.writeNumberField("p999", convertDuration(snapshot.get999thPercentile()));
			json.writeNumberField("count", timer.getCount());
			json.writeNumberField("m1_rate", convertRate(timer.getOneMinuteRate()));
			json.writeNumberField("m5_rate", convertRate(timer.getFiveMinuteRate()));
			json.writeNumberField("m15_rate", convertRate(timer.getFifteenMinuteRate()));
			json.writeNumberField("mean_rate", convertRate(timer.getMeanRate()));

			json.writeEndObject();
			json.flush();

			addReportBuffer(index, "timer", writer.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void reportMeter(String index, String timestamp, String name,
							 Metered meter) {
		try (StringWriter writer = new StringWriter()) {
			JsonGenerator json = createAndInitJsonGenerator(writer, timestamp, name);

			json.writeNumberField("m1_rate", convertRate(meter.getOneMinuteRate()));
			json.writeNumberField("m5_rate", convertRate(meter.getFiveMinuteRate()));
			json.writeNumberField("m15_rate", convertRate(meter.getFifteenMinuteRate()));
			json.writeNumberField("mean_rate", convertRate(meter.getMeanRate()));
			json.writeNumberField("count", meter.getCount());

			json.writeEndObject();
			json.flush();

			addReportBuffer(index, "meter", writer.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void reportHistogram(String index, String timestamp, String name,
								 Histogram histogram) {
		try (StringWriter writer = new StringWriter()) {
			JsonGenerator json = createAndInitJsonGenerator(writer, timestamp, name);

			final Snapshot snapshot = histogram.getSnapshot();

			json.writeNumberField("max", convertDuration(snapshot.getMax()));
			json.writeNumberField("mean", convertDuration(snapshot.getMean()));
			json.writeNumberField("min", convertDuration(snapshot.getMin()));
			json.writeNumberField("stddev", convertDuration(snapshot.getStdDev()));
			json.writeNumberField("p50", convertDuration(snapshot.getMedian()));
			json.writeNumberField("p75", convertDuration(snapshot.get75thPercentile()));
			json.writeNumberField("p95", convertDuration(snapshot.get95thPercentile()));
			json.writeNumberField("p98", convertDuration(snapshot.get98thPercentile()));
			json.writeNumberField("p99", convertDuration(snapshot.get99thPercentile()));
			json.writeNumberField("p999", convertDuration(snapshot.get999thPercentile()));
			json.writeNumberField("count", histogram.getCount());

			json.writeEndObject();
			json.flush();

			addReportBuffer(index, "histogram", writer.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void reportCounter(String index, String timestamp, String name,
							   Counter counter) {
		try (StringWriter writer = new StringWriter()) {
			JsonGenerator json = createAndInitJsonGenerator(writer, timestamp, name);

			long count = counter.getCount();
			json.writeNumberField("count", count);
			json.writeEndObject();
			json.flush();

			if (resettable) {
				counter.dec(count);
			}

			addReportBuffer(index, "counter", writer.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void reportGauge(String index, String timestamp, String name,
							 Gauge gauge) {

		Object value = gauge.getValue();
		if (value == null) {
			return;
		}

		try (StringWriter writer = new StringWriter()) {
			JsonGenerator json = createAndInitJsonGenerator(writer, timestamp, name);

			if (value instanceof Float) {
				json.writeNumberField("floatValue", (Float)value);
			} else if (value instanceof Double) {
				json.writeNumberField("doubleValue", (Double)value);
			} else if (value instanceof Byte) {
				// UNSUPPORTED
				// json.writeNumberField("byteValue", ((Byte) value).intValue());
				return;
			} else if (value instanceof Short) {
				json.writeNumberField("shortValue", (Short)value);
			} else if (value instanceof Integer) {
				json.writeNumberField("intValue", (Integer)value);
			} else if (value instanceof Long) {
				json.writeNumberField("longValue", (Long)value);
			} else {
				// UNSUPPORTED
				json.writeStringField("strings", value.toString());
				log.debug("A skip {} field, value={}", metricName(name), value.toString());
				return;
			}

			json.writeEndObject();
			json.flush();

			addReportBuffer(index, "gauge", writer.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private JsonGenerator createAndInitJsonGenerator(final Writer sw,
													 String timestamp, String name) throws IOException {
		JsonGenerator gen = jsonFactory.createGenerator(sw);
		gen.writeStartObject();
		gen.writeStringField(timestampFieldName, timestamp); // elasticsearch must be mappings `date` type.
		gen.writeStringField("@name", metricName(name));
		gen.writeStringField("hostname", this.hostname);
		if (null != additionalFields) {
			additionalFields.forEach((k, v) -> {
				try {
					gen.writeStringField(k, v);
				} catch (Exception e) {
				}
			});
		}
		return gen;
	}

	private String metricName(String... name) {
		return MetricRegistry.name(this.metricPrefixName, name);
	}

	protected void addReportBuffer(String index, String type, String json) {
		if (this.ttl > 0) {
			this.buffer.append(String.format(ES_BULK_INDEX_WITH_TTL_RAW_FORMAT, index, type, this.ttl.toString()));
		} else {
			this.buffer.append(String.format(ES_BULK_INDEX_RAW_FORMAT, index, type));
		}
		this.buffer.append("\n").append(json).append("\n");

	}

	protected void sendBulkRequestDummy() {
		final String sBuf = buffer.toString();
		if (Strings.isNullOrEmpty(sBuf)) {
			log.info("!! The metrics is blank !!");
			return;
		}
		log.info("=========== Elasticsearch '/_bulk' \n{}", sBuf);
	}

	protected void sendBulkRequest() {
		// Request to 'elasticsearch' bulk API. a.k.a: http://localhost:9200/_bulk

		HttpURLConnection connection = null;
		boolean connected = false;

		for (int i = 0; i < endpoint.size(); i++) { // Round-Robin
			int hostIndex = nextHost.get();
			nextHost.set((nextHost.get() == endpoint.size() - 1) ? 0 : nextHost.get() + 1);
			try {
				URL templateUrl = new URL("http://" + endpoint.get(hostIndex) + "/_bulk");
				log.info("Request to Elasticsearch '{}'", templateUrl.toString());
				connection = (HttpURLConnection)templateUrl.openConnection();
				connection.setRequestMethod("POST");
				connection.setConnectTimeout(3000); //3sec
				connection.setUseCaches(false);
				connection.setDoOutput(true);
				connection.connect();
				connected = true;
				break;
			} catch (IOException e) {
				log.error("Error connecting to {}: {}", endpoint.get(hostIndex), e);
			}
		}

		if (connected) {
			try {
				OutputStreamWriter printWriter = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
				printWriter.write(buffer.toString());
				printWriter.flush();
				printWriter.close();
				closeConnection(connection);

			} catch (Exception e) {
				log.warn("Fail! The metric reporting to Elasticsearch.", e);
			}
		}
	}

	private void closeConnection(HttpURLConnection connection)
			throws IOException {
		connection.getOutputStream().close();
		connection.disconnect();
		if (connection.getResponseCode() != 200) {
			log.warn("Reporting returned code {} {}: {}",
					connection.getResponseCode(),
					connection.getResponseMessage());
		}
	}
}