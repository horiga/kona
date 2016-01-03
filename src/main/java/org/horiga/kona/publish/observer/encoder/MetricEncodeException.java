package org.horiga.kona.publish.observer.encoder;


public class MetricEncodeException extends Exception {

	public MetricEncodeException(String message) {
		super(message);
	}

	public MetricEncodeException(Throwable cause) {
		super(cause);
	}

	public MetricEncodeException(String message, Throwable cause) {
		super(message, cause);
	}
}
