# План имплементации и конфигурации: модуль пакетной обработки прайс-листов

Верхнеуровневый пошаговый план. Итоговое решение - Spring Batch-приложение
(Spring Boot), запускаемое по расписанию k8s CronJob.

## Шаг 0. Подготовка окружения

* JDK 17, Maven или Gradle.
* PostgreSQL (существующая БД прайс-листов).
* Docker и кластер k8s (для локальной проверки - minikube).
* Prometheus / Grafana и ELK (уже используются в контуре).

## Шаг 1. Схема данных и индексы

1. Зафиксировать таблицы: `products`, `categories`, `clients`, `client_prices`.
2. Проверить внешние ключи и добавить индексы на поля соединения
   (`products.category_id`, `client_prices.product_id`,
   `client_prices.client_id`).
3. Вынести схему Spring Batch (`BATCH_JOB_INSTANCE`, `BATCH_JOB_EXECUTION` и
   др.) в ту же БД или в отдельную - она создаётся автоматически при старте.

## Шаг 2. Каркас приложения

1. Создать Spring Boot-проект (`spring-boot-starter-batch`,
   `spring-boot-starter-jdbc`, драйвер `postgresql`).
2. Добавить `spring-boot-starter-actuator` и
   `micrometer-registry-prometheus` (метрики), `logstash-logback-encoder`
   (JSON-логи для ELK).
3. Настроить подключение к БД через `application.yml`
   (параметры - из переменных окружения/Secret).

## Шаг 3. Конфигурация Spring Batch

1. Определить Job `priceListJob` и шаг `buildPriceListStep`.
2. Reader: `JdbcCursorItemReader` с SQL-соединением
   `products x categories x clients x client_prices`
   (курсор, чтобы не держать результат в памяти).
3. Processor: маппинг строки в DTO прайс-листа (наименование, категория,
   цена клиента, валюта, дата актуальности).
4. Writer: `FlatFileItemWriter`, разделитель `;`, заголовок CSV; файл
   формируется в временную директорию и затем загружается в объектное
   хранилище.
5. Задать размер чанка (например, 500) и `faultTolerant` (retry/skip).
6. При необходимости добавить `Partitioner` по `client_id` и `TaskExecutor`
   для параллельной генерации прайс-листов по клиентам.

## Шаг 4. Запись результата

1. Готовые CSV складывать в S3-совместимое объектное хранилище
   (путь вида `price-lists/YYYY-MM-DD/<client_id>.csv`).
2. Обеспечить идемпотентность: повторный запуск перезаписывает файлы за ту же
   дату, а не дублирует их.

## Шаг 5. Обработка ошибок

1. `retryLimit` и `backoffPolicy` для временных сбоев БД/хранилища.
2. `skipPolicy` для некорректных строк с записью в лог.
3. Включить restartability: при сбое Job перезапускается с последнего
   успешного чанка.

## Шаг 6. Расписание (k8s CronJob)

1. Собрать Docker-образ приложения и опубликовать в registry.
2. Создать манифест `CronJob` со `schedule: "0 6 * * *"`,
   `concurrencyPolicy: Forbid`, `restartPolicy: OnFailure`,
   `backoffLimit: 2`.
3. Секреты подключения к БД и к объектному хранилищу передать через
   `Secret`, настройки - через `ConfigMap`.
4. Задать `resources.requests/limits` для контейнера.

## Шаг 7. Наблюдаемость

1. Метрики: Actuator + Micrometer -> Prometheus (длительность Job и шагов,
   число прочитанных/записанных строк, ошибки).
2. Дашборд в Grafana и алерт на неуспешный запуск
   (`batch_job_execution_failed` / метрика статуса).
3. Логи: JSON через Logback -> Filebeat -> Logstash -> Elasticsearch ->
   Kibana; в каждой записи - `jobName`, `stepName`, `clientId`, `traceId`.

## Шаг 8. Проверка

1. Запустить Job вручную и убедиться, что CSV сформированы и загружены в
   хранилище.
2. Проверить корректность содержимого (число строк 5 000-10 000, цены
   соответствуют `client_prices`).
3. Проверить метрики в Prometheus/Grafana и логи в Kibana.
4. Проверить сценарии ошибок: недоступность БД (retry), повторный запуск
   (идемпотентность), перезапуск после сбоя (restart).

## Артефакты

| Артефакт | Назначение |
|---|---|
| Spring Boot-приложение (Spring Batch) | Batch-модуль с Job `priceListJob` |
| `application.yml` | Параметры БД, размер чанка, объектное хранилище |
| `Dockerfile` | Сборка образа приложения |
| Манифест `CronJob` | Ежедневный запуск в 06:00 |
| `ConfigMap` / `Secret` | Настройки и секреты |
| Дашборд Grafana + правила алертов | Мониторинг и оповещения |
| Конфигурация Filebeat/Logstash | Централизованное логирование |
