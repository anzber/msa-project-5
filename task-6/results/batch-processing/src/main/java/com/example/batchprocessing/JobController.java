package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * API запуска ETL-задачи. Задача больше не стартует вместе с приложением
 * (spring.batch.job.enabled=false), а запускается по этому endpoint.
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

	private static final Logger log = LoggerFactory.getLogger(JobController.class);

	private final Job job;
	private final JobLauncher jobLauncher;

	public JobController(Job job, JobLauncher jobLauncher) {
		this.job = job;
		this.jobLauncher = jobLauncher;
	}

	@PostMapping("/import-products")
	public ResponseEntity<Map<String, Object>> runImportProducts(
			@RequestParam(name = "source", required = false, defaultValue = "api") String source) throws Exception {

		log.info("API request received: source={}", source);

		JobParameters parameters = new JobParametersBuilder()
				.addLong("startedAt", System.currentTimeMillis())
				.addString("source", source)
				.toJobParameters();

		JobExecution execution = jobLauncher.run(job, parameters);

		Map<String, Object> body = new LinkedHashMap<>();
		body.put("jobName", execution.getJobInstance().getJobName());
		body.put("jobExecutionId", execution.getId());
		body.put("status", execution.getStatus().name());
		body.put("exitStatus", execution.getExitStatus().getExitCode());
		body.put("startTime", execution.getStartTime() == null ? null : execution.getStartTime().toString());
		body.put("endTime", execution.getEndTime() == null ? null : execution.getEndTime().toString());
		body.put("traceId", MDC.get("traceId"));
		body.put("spanId", MDC.get("spanId"));
		body.put("uri", MDC.get("uri"));

		log.info("API request finished: jobExecutionId={} status={}", execution.getId(), execution.getStatus());

		HttpStatus status = execution.getStatus().isUnsuccessful() ? HttpStatus.INTERNAL_SERVER_ERROR : HttpStatus.CREATED;
		return ResponseEntity.status(status).body(body);
	}
}
