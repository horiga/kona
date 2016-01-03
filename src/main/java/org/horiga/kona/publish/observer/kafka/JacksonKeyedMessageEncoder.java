package org.horiga.kona.publish.observer.kafka;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import com.netflix.servo.Metric;
import lombok.extern.slf4j.Slf4j;
import org.horiga.kona.publish.observer.encoder.JacksonMetricsEncoder;
import org.horiga.kona.publish.observer.encoder.MetricEncodeException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class JacksonKeyedMessageEncoder extends KeyedMessageEncoder<String> {

	final JacksonMetricsEncoder encoder;

	public JacksonKeyedMessageEncoder(String topic) {
		this(topic, null);
	}

	public JacksonKeyedMessageEncoder(String topic, ObjectMapper mapper) {
		super(topic);
		this.encoder = Objects.isNull(mapper) ? new JacksonMetricsEncoder() : new JacksonMetricsEncoder(mapper);
	}

	@Override
	String encodeMessage(List<Metric> metrics) throws MetricEncodeException {
		return encoder.encode(metrics);
	}
}
