COPY loyality_data (productSku, loyalityData)
FROM '/docker-entrypoint-initdb.d/loyality_data.csv'
WITH (FORMAT csv);
