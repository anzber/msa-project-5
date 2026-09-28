package com.example.batchprocessing;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Configuration
public class BatchConfiguration {

	@Bean
	public FlatFileItemReader<Product> reader() {
		return new FlatFileItemReaderBuilder<Product>()
			.name("productItemReader")
			.resource(new ClassPathResource("product-data.csv"))
			.delimited()
			.names("productId", "productSku", "productName", "productAmount", "productData")
			.targetType(Product.class)
			.build();
	}

	@Bean
	public ProductItemProcessor processor(JdbcTemplate jdbcTemplate) {
		return new ProductItemProcessor(jdbcTemplate);
	}

	@Bean
	public JdbcBatchItemWriter<Product> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Product>()
			.sql("INSERT INTO products (productId, productSku, productName, productAmount, productData) " +
				"VALUES (:productId, :productSku, :productName, :productAmount, :productData) " +
				"ON CONFLICT (productId) DO UPDATE SET " +
				"productSku = EXCLUDED.productSku, " +
				"productName = EXCLUDED.productName, " +
				"productAmount = EXCLUDED.productAmount, " +
				"productData = EXCLUDED.productData")
			.dataSource(dataSource)
			.beanMapped()
			.build();
	}

	@Bean
	public Job importProductJob(JobRepository jobRepository, Step step1, JobCompletionNotificationListener listener) {
		return new JobBuilder("importProductJob", jobRepository)
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.start(step1)
			.build();
	}

	@Bean
	public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
					  FlatFileItemReader<Product> reader, ProductItemProcessor processor, JdbcBatchItemWriter<Product> writer) {
		return new StepBuilder("step1", jobRepository)
			.<Product, Product>chunk(3, transactionManager)
			.reader(reader)
			.processor(processor)
			.writer(writer)
			.build();
	}

}
