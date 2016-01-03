package org.horiga.kona.publish.observer;

import com.netflix.servo.publish.AsyncMetricObserver;
import com.netflix.servo.publish.MetricObserver;

public final class Observers {

	private static int queueSize = 5000;
	private static long expireTimeMillis = 10 * 60 * 1000L; // 10 Minutes

	public static MetricObserver asyncObserver(String name, MetricObserver observer) {
		return new AsyncMetricObserver(name, observer, queueSize, expireTimeMillis);
	}

	private Observers() {
	}

}
