package org.horiga.kona.publish.observer.kafka;


import com.netflix.servo.Metric;
import com.netflix.servo.publish.BaseMetricObserver;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class KafkaMetricObserver<T> extends BaseMetricObserver {

	private final Producer<String, T> producer;
	private final String topic;
	private final KeyedMessageEncoder<T> encoder;

	public KafkaMetricObserver(String metricsName, Properties producerProperties,
							   String topic, KeyedMessageEncoder<T> encoder) {
		super(metricsName);
		Objects.requireNonNull(producerProperties, "Kafka producer properties must be not Null");
		this.producer = new Producer<String, T>(new ProducerConfig(producerProperties));
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				try {
					if (Objects.nonNull(producer))
						producer.close();
				} catch (Throwable e) {
					log.warn("Failed to kafka producer shutdown.", e);
				}
			}
		});
		this.topic = topic;
		this.encoder = encoder;
	}

	@Override
	public void updateImpl(List<Metric> list) {
		if(Objects.nonNull(producer)) {
			try {
				producer.send(encoder.encode(topic, list));
			} catch (Throwable e) {
				log.warn("Failed to publish the metrics to kafka brokers. topic:{}", topic, e);
			}
		}
	}
}
