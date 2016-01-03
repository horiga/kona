package org.horiga.kona.publish.observer.kafka;

import com.netflix.servo.Metric;
import kafka.producer.KeyedMessage;
import org.horiga.kona.publish.observer.encoder.MetricEncodeException;

import java.util.List;

public abstract class KeyedMessageEncoder<T> {

	protected final String topic;

	public KeyedMessageEncoder(String topic) {
		this.topic = topic;
	}

	public KeyedMessage<String, T> encode(String key, List<Metric> metrics) throws MetricEncodeException {
		KeyedMessage<String, T> message = new KeyedMessage<>(this.topic, key, encodeMessage(metrics));
		return message;
	}

	abstract T encodeMessage(List<Metric> metrics) throws MetricEncodeException;
}
