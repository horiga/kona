package org.horiga.kona.publish.observer.elasticsearch;


import com.netflix.servo.Metric;
import com.netflix.servo.publish.BaseMetricObserver;

import java.util.List;

public class ElasticsearchMetricObserver extends BaseMetricObserver {

	/*
	 * TODO:
	 */

	public ElasticsearchMetricObserver(String name) {
		super(name);
	}

	@Override
	public void updateImpl(List<Metric> list) {

	}
}
