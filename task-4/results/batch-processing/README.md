# Как собрать и запустить POC (Spring Batch ETL)

Проект: Spring Boot + Spring Batch. Читает `product-data.csv`, обогащает строки
данными о лояльности из таблицы `loyality_data` и пишет результат в таблицу
`products` (upsert по `productId`).

## 1. Сборка jar

```bash
./gradlew build -x test
```

Либо без локального JDK, в контейнере:

```bash
docker run --rm --user $(id -u):$(id -g) -e GRADLE_USER_HOME=/tmp/gradle \
  -v "$PWD":/project -w /project gradle:8.8-jdk17 gradle build --no-daemon -x test
```

## 2. Запуск PostgreSQL

```bash
docker compose up -d postgresdb
```

## 3. Создание таблиц и загрузка справочника

```bash
docker compose exec -T postgresdb psql -U postgres -d productsdb < src/main/resources/schema-all.sql

cat src/main/resources/loyality_data.csv | \
  docker compose exec -T postgresdb psql -U postgres -d productsdb \
  -c "COPY loyality_data (productSku, loyalityData) FROM STDIN WITH (FORMAT csv)"
```

## 4. Запуск ETL-задания

```bash
docker compose up --build app
```

Приложение запускает job `importProductJob`, выполняет шаг `step1` и
завершается. В логах видно обогащение строк (`Transforming ... into ...`),
завершение job и содержимое таблицы `products`.

## 5. Проверка результата

```bash
docker compose exec -T postgresdb psql -U postgres -d productsdb \
  -c "select * from products order by productId"
```

## Компоненты

* `Dockerfile` - образ приложения (eclipse-temurin:17-jre).
* `docker-compose.yml` - PostgreSQL + приложение.
* `.env` - параметры БД.
* `src/main/resources/schema-all.sql` - схема таблиц.
* `src/main/resources/product-data.csv` - входной CSV.
* `src/main/resources/loyality_data.csv` - справочник лояльности.
