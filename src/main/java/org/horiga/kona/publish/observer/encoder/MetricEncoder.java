package org.horiga.kona.publish.observer.encoder;

import com.netflix.servo.Metric;

import java.util.List;

public interface MetricEncoder<T> {

	T encode(List<Metric> metrics) throws MetricEncodeException;
}
