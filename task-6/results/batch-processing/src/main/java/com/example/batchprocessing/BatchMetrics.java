package com.example.batchprocessing;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Метрики пакетного приложения, доступные Prometheus через /actuator/prometheus.
 * Gauge-метрики регистрируются при первом завершении job: до первого запуска их
 * нет, поэтому алерт BatchJobFailed не срабатывает на ещё не запускавшемся job.
 */
@Component
public class BatchMetrics {

	private final MeterRegistry registry;
	private final AtomicInteger lastJobSuccess = new AtomicInteger(0);
	private final AtomicLong lastJobDurationMillis = new AtomicLong(0);
	private volatile boolean gaugesRegistered = false;

	public BatchMetrics(MeterRegistry registry) {
		this.registry = registry;
	}

	public void jobFinished(boolean success, long durationMillis) {
		registerGauges();
		lastJobSuccess.set(success ? 1 : 0);
		lastJobDurationMillis.set(durationMillis);
		Timer.builder("batch.job.duration")
				.description("Duration of importProductJob runs")
				.register(registry)
				.record(durationMillis, TimeUnit.MILLISECONDS);
	}

	public void productProcessed() {
		Counter.builder("batch.products.processed")
				.description("Number of products processed by the ETL step")
				.tag("job", "importProductJob")
				.register(registry)
				.increment();
	}

	private void registerGauges() {
		if (gaugesRegistered) {
			return;
		}
		synchronized (this) {
			if (gaugesRegistered) {
				return;
			}
			Gauge.builder("batch.job.last.success", lastJobSuccess, AtomicInteger::get)
					.description("1 if the last importProductJob finished COMPLETED, otherwise 0")
					.register(registry);
			Gauge.builder("batch.job.last.duration.millis", lastJobDurationMillis, AtomicLong::get)
					.description("Duration of the last importProductJob run in milliseconds")
					.register(registry);
			gaugesRegistered = true;
		}
	}
}
