package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;
	private final BatchMetrics metrics;

	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate, BatchMetrics metrics) {
		this.jdbcTemplate = jdbcTemplate;
		this.metrics = metrics;
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		log.info("!!! JOB STARTED! job={}", jobExecution.getJobInstance().getJobName());
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		boolean success = jobExecution.getStatus() == BatchStatus.COMPLETED;
		metrics.jobFinished(success, durationMillis(jobExecution));

		if (success) {
			log.info("!!! JOB FINISHED! Time to verify the results");
			jdbcTemplate
					.query("SELECT productId, productSku, productName, productAmount, productData FROM products",
							new DataClassRowMapper<>(Product.class))
					.forEach(product -> log.info("Transformed <{}> in the database.", product));
		} else {
			log.error("!!! JOB FAILED with status {}", jobExecution.getStatus());
		}
	}

	private long durationMillis(JobExecution jobExecution) {
		if (jobExecution.getStartTime() == null || jobExecution.getEndTime() == null) {
			return 0L;
		}
		return Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
	}
}
