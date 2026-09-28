package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public class ProductItemProcessor implements ItemProcessor<Product, Product> {

	private static final Logger log = LoggerFactory.getLogger(ProductItemProcessor.class);

	private final JdbcTemplate jdbcTemplate;
	private final BatchMetrics metrics;
	private final boolean simulateFailure;

	public ProductItemProcessor(JdbcTemplate jdbcTemplate, BatchMetrics metrics, boolean simulateFailure) {
		this.jdbcTemplate = jdbcTemplate;
		this.metrics = metrics;
		this.simulateFailure = simulateFailure;
	}

	@Override
	public Product process(final Product product) {
		if (simulateFailure && product.productId() == 3L) {
			// Демонстрационный отказ: нужен, чтобы показать срабатывание алерта batch_job_last_success == 0.
			throw new IllegalStateException("Simulated failure for productId=" + product.productId());
		}

		final Long productId = product.productId();
		final Long productSku = product.productSku();
		final String productName = product.productName();
		final Long productAmount = product.productAmount();
		final String productData = product.productData();

		// Данные о лояльности берём из справочной таблицы loyality_data по SKU товара.
		List<Loyality> loyality = jdbcTemplate.query(
				"SELECT productSku, loyalityData FROM loyality_data WHERE productSku = ?",
				new DataClassRowMapper<>(Loyality.class),
				productSku);

		String loyalityData = loyality.stream()
				.findFirst()
				.map(Loyality::loyalityData)
				.orElse(productData);

		Product transformedProduct = new Product(productId, productSku, productName, productAmount, loyalityData);

		metrics.productProcessed();
		log.info("Transforming ({}) into ({})", product, transformedProduct);

		return transformedProduct;
	}

}
