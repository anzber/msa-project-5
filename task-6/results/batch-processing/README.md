# Task 6: API запуска ETL-задачи и трейсинг в логах

Стек тот же, что в задаче 5 (Spring Batch + Prometheus + Grafana + ELK), но:

* ETL-задача больше не запускается при старте приложения
  (`spring.batch.job.enabled=false`);
* запуск выполняется по `POST /api/jobs/import-products` (`JobController`);
* в каждую строку лога добавлены `traceId`, `spanId` и `uri` (Micrometer Tracing
  + Brave, W3C Trace Context, плюс `RequestUriMdcFilter` для URI);
* клиент `../client/client.py` вызывает API и передаёт свой `traceparent`.

## Порты

Порты сдвинуты, чтобы не конфликтовать со стеком задачи 5 и другими сервисами
хоста.

| Сервис | Порт на хосте |
|---|---|
| app | 8082 -> 8080 |
| postgresdb | 5435 -> 5432 |
| prometheus | 9091 -> 9090 |
| grafana | 3002 -> 3000 |
| kibana | 5602 -> 5601 |
| elasticsearch | 9211 -> 9200 |
| logstash | 5045 -> 5044 |

## Запуск

```bash
# сборка jar
docker run --rm --user $(id -u):$(id -g) -e GRADLE_USER_HOME=/tmp/gradle \
  -v "$PWD":/project -w /project gradle:8.8-jdk17 gradle build --no-daemon -x test

# поднять стек
docker compose up -d --build

# вызвать API клиентом
python3 ../client/client.py --url http://localhost:8082 --source python-client
```

## Проверка трейсинга

Ответ API содержит `traceId`, `spanId` и `uri`. Тот же `traceId` находится в
Kibana (Discover, index-pattern `filebeat-*`) по запросу
`traceId: "<значение>"`; в логах видно весь путь запроса, включая шаги job.
