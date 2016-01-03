package org.horiga.kona.publish.observer.encoder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.netflix.servo.Metric;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class JacksonMetricsEncoder implements MetricEncoder<String> {

	public static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper()
			.registerModules(new AfterburnerModule(), new JavaTimeModule())
			.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
			.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, true)
			.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, true)
			.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
			.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
			.setSerializationInclusion(JsonInclude.Include.NON_NULL);

	private final ObjectMapper mapper;

	public JacksonMetricsEncoder() {
		this(DEFAULT_MAPPER);
	}

	public JacksonMetricsEncoder(ObjectMapper mapper) {
		Objects.requireNonNull(mapper, "jackson object mapper must be specified.");
		this.mapper = mapper;
	}

	@Override
	public String encode(List<Metric> metrics) throws MetricEncodeException {
		try {
			Map<String, Object> values = values();
			metrics.stream().forEach(m -> values.put(m.getConfig().getName(), m.getValue()));
			final String encoded = mapper.writeValueAsString(values);
			if(log.isDebugEnabled()) {
				log.debug("metrics json encoded value: {}", encoded);
			}
			return encoded;
		} catch (JsonProcessingException e) {
			throw new MetricEncodeException(e);
		}
	}

	protected Map<String, Object> values() {
		Map<String, Object> values = new HashMap<>();
		return values;
	}
}
